import pandas as pd
import time
import matplotlib.pyplot as plt
import os

download_path='/'.join( os.getcwd().split('\\')[:3] ) + '/Downloads/enron-event-history-all/enron-event-history-all.csv'
upload_path = '/'.join( os.getcwd().split('\\')[:3] ) + '/Downloads/enron-event-history-all'

## Reading CSV with Pandas Dataframe
df = pd.read_csv(download_path)

## Printing Info about DataFrame
##print(df.info())

## Number of Rows in DataFrame - 205730
#print(len(df))


## Now adding column names to dataframe without replacing data on row 1

df.loc[-1]= df.columns
df.index = df.index + 1

## Adding column names to the data frame
df.columns = ['Time', 'Message identifier', 'Sender', 'Recipients', 'Topic', 'Mode']

## Number of Rows increased by 1 as columns have been added - 215731
#print(len(df))

## Prints elements on first row
#print(df.loc[0])

## Now changing Unix DateTime to Readable Date Time
df['Time'] = pd.to_datetime(df['Time'], unit='ms')

## Extracting Only required columns in a new dataframe
df2 = df[['Time', 'Sender', 'Recipients'] ]

##df.columns[2:4]]
#print("Now Printing New DataFrame")


##Now Printing Sender Counts in a dictionary

Send = df2['Sender'].value_counts().to_dict()


df3= df2[['Time', 'Recipients']]


li = df3['Recipients'].to_list()

Rec={}
c=[]
for i in li:
    j=(str(i).split('|'))
    c.extend(j)
for i in c:
    if i in Rec.keys():
        Rec[i]+=1
    else:
        Rec[i]=1

dfCSV=pd.DataFrame(Send, index = ['send']).transpose()


dfCSV['received'] = 0

for i,j in dfCSV.iterrows():
    if i not in Rec.keys():
        continue
    elif i in Rec.keys():
        dfCSV['received'][i]=Rec[i]

dfCSV.index.names = ['person']

##1) Output to CSV File
dfCSV.to_csv(upload_path +'/File_With_3_Columns.csv')

print("Output File has been created for part #1")

## 2) PNG Image

df2['year'] = pd.DatetimeIndex(df2['Time']).year
df2['month'] = pd.DatetimeIndex(df2['Time']).month

z = df2.groupby(['Sender','year','month']).size()


#z2=z.groupby(level=[0,1]).nlargest(5)

z2=df2.groupby(['year'],sort=True).month.value_counts()
z2.sort_index(ascending=True)
#print(z2)

z2.plot(title='Emails sent vs Time-period')
plt.xlabel('Year & Month')
plt.ylabel('# of Emails')
plt.savefig(upload_path+'/Emails_Sent_Vs_Time_Period.png')


plt.clf()
z3=df2.groupby(['Sender'],sort=True).year.value_counts().nlargest(10)

z3.sort_index(ascending=True)

z3.plot(title='Top 10 Email Senders with Year')
plt.xticks(fontsize=6,rotation = 20)
plt.xlabel('Sender & Year')
plt.ylabel('# of Emails Sent')
plt.savefig(upload_path+'/Top_10_Email_Senders_With_Year.png')

print("Two plots have been saved for part #2")
## 3 Unique Value Metrics

df5=pd.DataFrame(df2.groupby(['year']).agg(['nunique']))

df5.to_csv(upload_path +'/Nunique_Metrics.csv')

df7 = pd.read_csv(upload_path+'/Nunique_Metrics.csv')

df7 = df7.iloc[2:]
Send_Val=list(df7['Sender'])

Ind = df5.index.values
Unique_Send = df5.columns
plt.clf()
Plot_Dict = {}
#print(Send_Val)
for i in range(len(Ind)):
    Plot_Dict[Ind[i]]=int(Send_Val[i])
df6=(df.iloc[:, df5.columns.get_level_values(1)=='nunique'])

plt.bar(range(len(Plot_Dict)), list(Plot_Dict.values()), align='center')
plt.xticks(range(len(Plot_Dict)), list(Plot_Dict.keys()))
plt.title('Trend of Unique Senders by Year')
plt.xlabel('Year')
plt.ylabel('# of Unique Senders')
plt.savefig(upload_path+'/Unique_Senders.png')
##plt.show()
print("Plot has been saved for part #3")







