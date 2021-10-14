import datetime as dt

epoch = 1632201592
print(epoch, '-------------mi- epoch')

#mytime = datetime.fromtimestamp(epoch)
mytime = dt.datetime.fromtimestamp(epoch)

print(mytime, '----- converted')

myutc = mytime.strftime('%Y-%m-%dT%H:%M:%SZ')

print( myutc, '---- utc time')

mygmt2 = mytime.strftime('%Y-%m-%dT%H:%M:%S+02:00')

print(mygmt2,'gmt+2 time')