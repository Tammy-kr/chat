import telegram
from datetime import datetime, timedelta, date
import io
import requests

import pandas as pd
import pandahouse as ph
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.pyplot import figure

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# defaults
default_args = {
    'owner': '####',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2022, 1, 30)
}

# connection ClickHouse
connection = {'host': 'https://####',
                      'database': '####',
                      'user': '####',
                      'password': '####'
                      }

# DAG start interval
schedule_interval = '0 11 * * *'

# bot and chat information
chat_id = ####
my_token = '####'
bot = telegram.Bot(token=my_token)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_task():
    
     #upload data for the week from feed_actions
    @task
    def get_feed_actions():
        q1 = '''
            SELECT
                toDate(time) AS event_date,
                count(DISTINCT user_id) AS dau,
                countIf(action='like') AS likes,
                countIf(action='view') AS views,
                likes / dau AS likes_per_user,
                likes / dau AS views_per_user,
                likes / views AS ctr
            FROM
                {db}.feed_actions
            WHERE
                toDate(time) between today() - 7 and yesterday()
            GROUP BY
                event_date
            ORDER BY 
                event_date DESC'''
        
        result_feed = ph.read_clickhouse(q1,connection=connection)
        return result_feed
    
        #upload data for the week from message_actions
    @task
    def get_message_actions():
        q2 = '''
            SELECT
                toDate(time) AS event_date,
                uniqExact(user_id) sender_users,
                count(user_id) AS messages,
                messages / sender_users as messages_per_user
            FROM
                {db}.message_actions
            WHERE
                toDate(time) between today() - 7 and yesterday()
            GROUP BY
                event_date
            ORDER BY 
                event_date DESC'''
        
        result_msg = ph.read_clickhouse(q2,connection=connection)
        return result_msg
    
        # join tables
    @task
    def join_feed_msg(result_feed, result_msg):
        result = result_feed.merge(result_msg, how='inner', on='event_date')
        return result
    
     #information about the values of key metrics for the previous day
    @task
    def get_message_last_day(result, chat_id):
        day = result['event_date'].iloc[0]
        dau_feed = result['dau'].iloc[0]
        dau_msg = result['sender_users'].iloc[0]
        likes = result['likes'].iloc[0]
        views = result['views'].iloc[0]
        ctr = round(result['ctr'].iloc[0], 2)
        msg = result['messages'].iloc[0]
        
        yestarday_date = datetime.now() - timedelta(days=1)
        
        message = (
            f"""
        Добрый день, значения ключевых метрик ленты новостей и мессенджера за последние 7 дней:
        по состоянию на дату: {yestarday_date.strftime('%d-%m-%Y')},
        1. DAU ленты новостей: {dau_feed:,},
        2. DAU мессенджера: {dau_msg:,}
        3. Лайки: {likes:,},
        4. Просмотры: {views:,},
        5. CTR ленты новостей: {ctr:.1f},
        6. Отправлено сообщений: {msg:,}"""
                   .replace(',', ' ')
                  )
        bot.sendMessage(chat_id=chat_id, text=message)
    
    
    @task
    def get_photo_last_week(result, chat_id):
        fig, ax = plt.subplots(2, 2, figsize=(30, 15))
        fig.suptitle('Динамика показателей за последние 7 дней', fontsize=25)
        
    
        sns.lineplot(ax=ax[0,0], data = result, x='event_date', y='dau', label = 'DAU ленты новостей', linewidth = 3, color = '#A61F69')
        sns.lineplot(ax=ax[0,0], data = result, x='event_date', y='sender_users', label = 'DAU мессенджера', linewidth = 3, color = '#400E32')
        ax[0,0].set_title('DAU ленты новостей и мессенджера',fontsize=10)
        ax[0,0].legend()
        ax[0,0].grid()
        
        
        sns.lineplot(ax=ax[0,1], data = result, x='event_date', y='ctr', label = 'CTR', linewidth = 3, color = '#F2921D')
        ax[0,1].set_title('CTR ленты новостей', fontsize=10)
        ax[0,1].grid()
        
        
        sns.lineplot(ax=ax[1,0], data = result, x='event_date', y='likes', label = 'Лайки', linewidth = 3, color='#AF0171')
        sns.lineplot(ax=ax[1,0], data = result, x='event_date', y='views', label = 'Просмотры', linewidth = 3, color='#790252')
        sns.lineplot(ax=ax[1,0], data = result, x='event_date', y='messages', label = 'Сообщения', linewidth = 3, color='#E80F88')
        ax[1,0].set_title('События', fontsize=10)
        ax[1,0].legend()
        ax[1,0].grid()
        
        
        sns.lineplot(ax = ax[1,1], data = result, x = 'event_date', y = 'likes_per_user', label = 'Лайки', linewidth = 3, color='#FF7000')
        sns.lineplot(ax = ax[1,1], data = result, x = 'event_date', y = 'views_per_user', label = 'Просмотры', linewidth = 3, color='#540375')
        sns.lineplot(ax = ax[1,1], data = result, x = 'event_date', y = 'messages_per_user', label = 'Сообщения', linewidth = 3, color='#10A19D')
        ax[1,1].set_title('События на 1 пользователя')
        ax[1,1].grid()
        plt.tight_layout()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'task_7_2.png'
        plt.close()
        
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        # tasks

    result_feed = get_feed_actions()
    result_msg = get_message_actions()
    result = join_feed_msg(result_feed, result_msg)
    get_message_last_day(result, chat_id)
    get_photo_last_week(result, chat_id)
    
dag_task() = dag_task()