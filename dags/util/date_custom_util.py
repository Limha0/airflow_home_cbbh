from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import MonthEnd

now = datetime.now()

class DateUtil:
    def get_yyyy():
        """
        현재 연도 조회
        """
        return now.strftime("%Y")
    
    def  get_yyyymm():
        """
        현재 연월 조회
        """
        return now.strftime("%Y%m")

    def get_yyyymmdd():
        """
        현재 연월일 조회
        """
        return now.strftime("%Y%m%d")
    
    def get_ymdhm():
        """
        현재 연월일시분 조회
        """
        return now.strftime("%Y%m%d%H%M")

    def get_ymdhms():
        """
        현재 연월일시분초 조회
        """
        return now.strftime("%Y%m%d%H%M%S")
    
    def get_ymdhmss():
        """
        현재 연월일시분초밀리초 조회
        """
        return now.strftime("%Y%m%d%H%M%S%f")[:-3]

    def get_month_ago_date():
        """
        한 달 전 조회
        """
        return (now - relativedelta(months=1)).strftime("%Y%m")

    def get_2month_ago_date():
        """
        두 달 전 조회
        """
        return (now - relativedelta(months=2)).strftime("%Y%m")

    def get_3month_ago_date():
        """
        세 달 전 조회
        """
        return (now - relativedelta(months=3)).strftime("%Y%m")


    def get_5min_ago_date():
        """
        5분 전 조회
        """
        return (now - relativedelta(minutes=5)).strftime("%Y%m%d%H%M")

    def get_day_ago_date():
        """
        하루 전날 조회
        """
        return (now - relativedelta(days=1)).strftime("%Y%m%d")

    def get_last_day_of_month_ago():
        """
        한 달 전의 마지막 날짜
        """
        return (now - relativedelta(months=1)+MonthEnd(0)).strftime("%Y%m%d%H%M")

    def get_between_days(start_date, end_date):
        """
        두 날짜 사이의 일 수
        params: start_date, end_date (DateTime)
        """
        return abs((end_date - start_date).days)
    
    def get_date_list(start_date, end_date):
        """
        두 날짜 사이의 날짜 리스트 (start_date부터 end_date 전날까지)
        params: start_date, end_date (DateTime)
        return: date_list
        """
        delta = timedelta(days=1)
        date_list = []
        start_date = datetime.strptime(start_date.strftime('%Y%m%d'),'%Y%m%d').date()
        end_date = datetime.strptime(end_date.strftime('%Y%m%d'),'%Y%m%d').date()
        while start_date < end_date:
            date_list.append(start_date.strftime('%Y%m%d'))
            start_date += delta
        return date_list
