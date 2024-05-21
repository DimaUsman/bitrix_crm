import os

import re
import locale
import mysql.connector
from dotenv import load_dotenv  
import pandas as pd
import sqlalchemy

load_dotenv()

sql = f"""
select b_crm_deal.ID,
b_crm_company.TITLE as COMPANY_NAME ,
	b_crm_deal.DATE_CREATE ,
	b_crm_deal.CREATED_BY_ID ,
	b_crm_deal.ASSIGNED_BY_ID ,
	b_crm_deal.BEGINDATE ,
	b_crm_deal.OPENED ,
	b_crm_deal.CLOSEDATE ,
	b_crm_deal.CLOSED ,	
--	b_crm_deal.TITLE ,
	b_crm_deal_category.NAME AS CATEGORY_NAME ,
	b_crm_deal.STAGE_ID ,
	b_crm_deal.STAGE_SEMANTIC_ID ,
	b_crm_deal.IS_NEW ,
	b_crm_deal.IS_RECURRING ,
	b_crm_deal.IS_RETURN_CUSTOMER ,
	b_crm_deal.IS_REPEATED_APPROACH ,
	b_crm_deal.IS_MANUAL_OPPORTUNITY ,
    b_crm_deal.TYPE_ID,
	b_crm_deal.OPPORTUNITY ,
	b_crm_deal.TAX_VALUE ,
	b_crm_deal.CURRENCY_ID ,
	b_crm_deal.EXCH_RATE ,
    b_crm_webform.name AS WEBFORM_NAME,
	b_crm_deal.SOURCE_ID,
    b_crm_contact.FULL_NAME AS CONTACT_NAME,
    b_crm_deal.CONTACT_ID
from b_crm_deal left join b_crm_company on b_crm_deal.COMPANY_ID = b_crm_company.ID  
	left join b_crm_deal_category on b_crm_deal_category.ID = b_crm_deal.CATEGORY_ID
    left join b_crm_webform on b_crm_webform.ID = b_crm_deal.WEBFORM_ID
    left join b_crm_contact on b_crm_contact.ID = b_crm_deal.CONTACT_ID
"""

db_link = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(
    os.getenv("LOGIN_BITRIX"),
    os.getenv("PASSWORD_BITRIX"),
    os.getenv("HOST_BITRIX2"),
    os.getenv("PORT_BITRIX"),
    os.getenv("DBNAME_BITRIX")  
    )

engine = sqlalchemy.create_engine(db_link)
df = pd.read_sql_query(sql, engine)

sql = f"""
    SELECT b_crm_lead.ID,
	b_crm_lead.DATE_CREATE ,
	b_crm_lead.ASSIGNED_BY_ID ,
	b_crm_lead.STATUS_ID ,
	b_crm_lead.STATUS_SEMANTIC_ID ,
	b_crm_lead.SOURCE_ID ,
	b_crm_lead.DATE_CLOSED ,
	b_crm_lead.IS_RETURN_CUSTOMER,
    b_crm_contact.FULL_NAME AS CONTACT_NAME,
    b_crm_lead.CONTACT_ID,
    b_crm_company.TITLE as COMPANY_NAME ,
     b_crm_webform.name AS WEBFORM_NAME
from b_crm_lead   
	left join b_crm_company on b_crm_lead.COMPANY_ID = b_crm_company.ID  
    left join b_crm_webform on b_crm_webform.ID = b_crm_lead.WEBFORM_ID
	left join b_crm_contact on b_crm_contact.ID = b_crm_lead.CONTACT_ID
"""
df_lead = pd.read_sql_query(sql, engine)

sql = f"""
SELECT b_crm_webform_counter_daily.DATE_STAT,
    b_crm_webform.name as WEBFORM_NAME,
    b_crm_webform_counter_daily.VIEWS,
    b_crm_webform_counter_daily.START_FILL,
    b_crm_webform_counter_daily.END_FILL
FROM b_crm_webform_counter_daily
    left join b_crm_webform on b_crm_webform.ID = b_crm_webform_counter_daily.FORM_ID
"""
df_webform_daily = pd.read_sql_query(sql, engine)

sql = f"""
select id,
	name,
	is_system,
	COPYRIGHT_REMOVED
from b_crm_webform
"""
df_webform_desc = pd.read_sql_query(sql, engine)

db_link = "postgresql://{}:{}@{}:5432/{}".format(
    os.getenv("LOGIN_DATAOFFICE"),
    os.getenv("PASSWORD_DATAOFFICE"),
    os.getenv("HOST_GP"),
    os.getenv("DBNAME_PROD")  
    )
engine = sqlalchemy.create_engine(db_link)

sql_query = f"""
select createddate::date as rate_date, 
	(avg(exchrate) / 100) as rate
from stg_ax.t_000005_adb_marvel_exchrates tame 
where lower(currencycode) = 'usd' and lower(dataareaid) = 'rur'
group by createddate
order by createddate desc
"""
df_rates = pd.read_sql_query(sql_query, engine)


def round_to_monday(date):
    if date.weekday() ==0:
        return date.date()
    return (date - pd.DateOffset(days=(date.weekday()))).date()

def round_to_monday_(date):
    if date.weekday() ==0:
        return date
    return (date - pd.DateOffset(days=( date.weekday()))).date()

def prepare_table(df, date_col, cond):
    # Save the original locale
    original_locale = locale.getlocale(locale.LC_TIME)
    # Set the locale to Russian
    locale.setlocale(locale.LC_TIME, 'ru_RU.utf8')
    
    for col in date_col:      
        df.columns = [word.lower() for word in df.columns]
        df[col] = pd.to_datetime(df[col])
        df[col + '_week'] = df[col].apply(round_to_monday)
        df[col + '_dow'] = (1 + df[col].dt.day_of_week).astype(str) + '. ' + df[col].dt.day_name()

    # Restore Locale
    locale.setlocale(locale.LC_TIME, original_locale)
    return df

df_lead.columns = ['id', 'begindate', 'assigned_by_id', 'status_id',
       'status_semantic_id', 'source_id', 'closedate', 'is_return_customer',
       'contact_name', 'contact_id', 'company_name', 'webform_name']

df_webform_daily.columns = ['begindate', 'form_name', 'views', 'start_fill', 'end_fill']

df = prepare_table(df, ['begindate' , 'closedate'],True)
df_lead = prepare_table(df_lead, ['begindate' , 'closedate'],True)
df_webform_daily = prepare_table(df_webform_daily, ['begindate'], False)

df_rates['date_week'] = df_rates['rate_date'].apply(round_to_monday_)
df_rate = df_rates.groupby('date_week')['rate'].mean()
df_rate = df_rate.reset_index()
df = df.merge(df_rate, left_on = 'begindate_week', right_on='date_week')
df['opportunity'] = df.apply(lambda line: line['opportunity'] * line['rate'] if line['currency_id'] =='USD' else  line['opportunity'], axis=1)


def transform_form_name(df, col_name):
    # 
    df[col_name] = df[col_name].str.replace(r'.*Умный дом.*', 'Заявка на продажу "умного дома"',regex=True)
    df[col_name] = df[col_name].str.replace(r'.*Контактные данные.*', 'Контактные данные',regex=True)
    df.drop(
         df[
             df[col_name].isin(['Тест для ДЭК', '24 марта, проверка работы "продажи умного дома"'])
             ].index, inplace=True
    )
    
    return df

df = transform_form_name(df, 'webform_name')
df_lead = transform_form_name(df_lead, 'webform_name')
df_webform_daily = transform_form_name(df_webform_daily, 'form_name')

def prepare_stage_semantic_id(df, col_name):
    df[col_name] = df[col_name].str.replace(r'F', 'Сделка проиграна',regex=True)
    df[col_name] = df[col_name].str.replace(r'S', 'Сделка выиграна',regex=True)
    df[col_name] = df[col_name].str.replace(r'P', 'Сделка незавершена',regex=True)
    return df

df = prepare_stage_semantic_id(df, 'stage_semantic_id')
df_lead = prepare_stage_semantic_id(df_lead, 'status_semantic_id')

def fix_webform(df, df_webform_desc):
    df['og_source_id'] = df.source_id
    df.source_id = df['source_id'].apply(lambda x: 'WEBFORM' if (x != 'EMAIL') & (x!= 'ADVERTISING') & (x!='CALL') & (x!='мерояприятие') & (x is not None) else x )
    df.webform_name = df.apply(
        lambda line: line.webform_name if (line.og_source_id is None) or (re.search(r'[0-9]+.*', line.og_source_id) is None)
            else df_webform_desc.loc[df_webform_desc.id == int(re.split(r'([0-9]+).*', line.og_source_id )[1]), 'name'].values[0]
        , axis=1
    )
    df.drop(columns = ['og_source_id'], inplace=True)
    return df

df_lead = fix_webform(df_lead,df_webform_desc)
df = fix_webform(df,df_webform_desc)

with pd.ExcelWriter('webform_crm.xlsx') as writer:
    # Write the first DataFrame to the first sheet
    df.to_excel(writer, sheet_name='deals', index=False)
    
    # Write the second DataFrame to the second sheet
    df_lead.to_excel(writer, sheet_name='leads', index=False)
    df_webform_daily.to_excel(writer, sheet_name='webform', index=False)