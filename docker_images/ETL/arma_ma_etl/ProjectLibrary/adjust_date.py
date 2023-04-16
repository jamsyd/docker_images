import datetime

def storeDates(date=None,path=None):

    import sys
    from datetime import date

    # Setting date
    # record_date = date.today()
    record_date = date.today().strftime("%Y-%m-%d %H:%M:%S")

    hs = open("/home/ec2-user/docker_images/trading_dashboard/settings/train_dates.txt","a")
    hs.write(str(record_date) + "\n")
    hs.close()
