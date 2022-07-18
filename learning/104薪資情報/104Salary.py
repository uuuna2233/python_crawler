import requests
from fake_useragent import UserAgent
import pandas as pd
from datetime import datetime
import pymysql

# Connect to the database

db = pymysql.connect(host='127.0.0.1', user='root', password='root', port=3306, db='104')
cursor = db.cursor(pymysql.cursors.DictCursor)

headers = {'Content-Type': 'application/json',
           'User-Agent': UserAgent().chrome}
url = 'https://be.guide.104.com.tw/api/keyword/seniority'


def get_better_salary(keyword):
    '''獲得 104 薪資情報'''
    
    quary = {"type": "1", "keyword":[keyword]}
    r = requests.post(url, json = quary, headers = headers)
    
    try:
        r.raise_for_status()
   
        data = r.json()
        count = data['sampleCount']
    
        if count != 0:
            update = data['updateDate']
            
            seniority = ['1年以下','1~3年','3~5年','5~10年','10年以上']
            salary = ['平均月薪','P25月薪','P50月薪','P75月薪','職缺數量']
            index = pd.MultiIndex.from_product([seniority, salary])
            
            salary_value = ['salary', 'salary25', 'salary50', 'salary75', 'jobCount']
            
            result = []
            for i in range(5):
            	for j in salary_value:
            		result.append(data['salaryList'][i][j])

            df = pd.DataFrame(result, index = index, columns = [keyword]).round(0)
            return df, result, update

        else:
            print('「', keyword, '」樣本不足無法分析!')  
            return pd.DataFrame(), [], None
    
    except:
            print('抱歉! 沒有 「', keyword, '」 職位名稱!')
            return pd.DataFrame(), [], None
        

if __name__ == "__main__":

    salaryList = pd.DataFrame()
    salaryResult = []
    
    # 將欲查詢的職務薪資放進 txt 並讀取
    with open('jobsalary.txt','r',encoding="utf-8") as f:
        lines = f.readlines()
        words = [word.strip() for word in lines]
    
    num = 0
    for keyword in words:
        df, result, update = get_better_salary(keyword)
        num += 1
        if not df.empty:
            if num == 1:
                salaryList = df
                upDate = datetime.fromtimestamp(update/1000).strftime('%Y-%m-%d')
            else:
                salaryList = pd.concat([salaryList, df], axis = 1, join = 'outer')
        else:
            continue
        
        if result != []:
            salaryResult.append(result)
            
    # save to csv
    if not salaryList.empty:
        file_name = '104Salary_' + upDate + ('.csv')
        salaryList.to_csv(file_name, encoding = 'utf_8_sig')
        print('你所查詢的薪資情報已成功存為csv檔!')
        
    # import to mySQL
    try:
        with db.cursor() as cursor:
            if salaryResult != []:
                num = 0
                for s in salaryResult:
                    # 若職缺某年資樣本數不足，會顯示 NaN，而 mySQL 無法辨識 NaN，需取代為 Null
                    s = ['NULL' if i == 'NaN' else i for i in s]
                    try:
                        sql = f'''INSERT INTO `dream`.`salary`
                        (`jobtitle`,`1_salaryAvg`,`1_salary25`,`1_salary50`,`1_salary75`,`1_jobCount`,`1~3_salaryAvg`,`1~3_salary25`,`1~3_salary50`,`1~3_salary75`,`1~3_jobCount`,`3~5_salaryAvg`,`3~5_salary25`,`3~5_salary50`,`3~5_salary75`,`3~5_jobCount`,`5~10_salaryAvg`,`5~10_salary25`,`5~10_salary50`, `5~10_salary75`,`5~10_jobCount`,`10_salaryAvg`,`10_salary25`,`10_salary50`,`10_salary75`,`10_jobCount`,`updatetime`) VALUES
                        ("{words[num]}",{s[0]},{s[1]},{s[2]},{s[3]},{s[4]},{s[5]},{s[6]},{s[7]},{s[8]},{s[9]},{s[10]},{s[11]},{s[12]},{s[13]},{s[14]},{s[15]},{s[16]},{s[17]},{s[18]},{s[19]},{s[20]},{s[21]},{s[22]},{s[23]},{s[24]},"{upDate}")'''
                        
                        cursor.execute(sql)
                        num += 1
                    except Exception as e:
                        print(e)
                        num += 1
                        continue
        db.commit()
    
    finally:
        db.close()
        print('你所查詢的薪資情報已成功匯入mySQL!')
