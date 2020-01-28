import csv
import argparse
import datetime
import calendar

row = ['DateKey', 'CalendarDate', ' Date', ' Weekday', ' WeekDayName', ' WeekDayNameShortName', ' DayOfYear',
       'WeekOfYear', 'Month', 'MonthName', 'MonthNameShort', 'Quarter', 'QuarterName', 'Year', 'IsWeekend']


def createFile(fname, type):
    yr = 2010
    d1 = datetime.datetime(yr, 1, 3)
    if calendar.isleap(int(yr)):
        count = 366
    else:
        count = 365

    # for fileCnt in range(parts):
    with open(fname + "_" + str(yr) + ".csv", 'w+') as writeFile:
        while yr <= 2011:
            writer = csv.writer(writeFile)
            writer.writerow(row)
            for x in range(count):
                DateKey = d1.strftime("%Y%m%d")
                ddate = d1.date()
                dday = d1.day
                Weekday = d1.strftime("%w")
                WeekDayName = d1.strftime("%A")
                WeekDayNameShortName = d1.strftime("%a")
                # DOWInMonth not done
                DayOfYear = d1.strftime("%j")
                # WeekOfMonth not done
                WeekOfYear = d1.strftime("%U")
                Month = d1.strftime("%m")
                MonthName = d1.strftime("%B")
                MonthNameShort = d1.strftime("%b")
                Quarter = ((int(Month) - 1) // 3 + 1)
                QuarterName = 'Q' + str(Quarter)
                Year = d1.strftime("%Y")
                # MMYYYY
                # MonthYear
                # IsWeekend
                if int(Weekday) in [0, 6]:
                    IsWeekend = 'Y'
                else:
                    IsWeekend = 'N'

                row[0] = DateKey
                row[1] = ddate
                row[2] = dday
                row[3] = Weekday
                row[4] = WeekDayName
                row[5] = WeekDayNameShortName
                row[6] = DayOfYear
                row[7] = WeekOfYear
                row[8] = Month
                row[9] = MonthName
                row[10] = MonthNameShort
                row[11] = Quarter
                row[12] = QuarterName
                row[13] = Year
                row[14] = IsWeekend

                writer.writerow(row)
                d1 += datetime.timedelta(days=1)
            yr += 1
            if calendar.isleap(int(yr)):
                count = 366
            else:
                count = 365
        writeFile.close()


if __name__ == "__main__":
    createFile(fname='calendar', type='overwrite')
    print("Files created")
else:
    print("exiting here...")
