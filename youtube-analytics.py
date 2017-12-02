import boto3
from boto3.dynamodb.conditions import Key, Attr
import requests
from decimal import *
from datetime import datetime, timedelta
import isodate
from googleads import adwords
from googleads import oauth2
import time
import Queue
import threading


class Video(object):
    """
    Youtube Analytics for channels and respective videos
    """ 
    client_id = "xxxxx"
    client_secret = "xxxxx"

    def __init__(self, channelID, apiUrl, startDate, endDate, refreshToken, videoid ):
        self.channelID = channelID
        self.apiUrl = apiUrl
        self.startDate = startDate
        self.endDate = endDate
        self.refreshToken = refreshToken
        self.videoid = videoid

    def getBasicData(self):
        print "Calling basic data ======>"
        dataurl = "ids=channel==" + self.channelID + "&start-date="+ self.startDate+"&end-date="+ self.endDate+ "&filters=video==" + self.videoid + "&access_token=" + self.accessToken
        metrics = "likes,views,annotationClickThroughRate,shares,comments,dislikes,annotationClicks,averageViewDuration,averageViewPercentage,videosAddedToPlaylists,subscribersGained,subscribersLost,estimatedMinutesWatched,cardClicks"
        yturl =  self.apiUrl + dataurl + "&metrics=" + metrics + "&sort=-views"
        response_total = requests.get(yturl)
        response_total = response_total.json()
        self.likes = str(response_total["rows"][0][0])
        self.views = str(response_total["rows"][0][1])
        self.annotationClickThroughRate  = str(response_total["rows"][0][2])
        self.shares = str(response_total["rows"][0][3])
        self.comments = str(response_total["rows"][0][4])
        self.dislikes = str(response_total["rows"][0][5])
        self.annotationClicks = str(response_total["rows"][0][6])
        self.averageViewDuration = str(response_total["rows"][0][7])
        self.averageViewPercentage = str(response_total["rows"][0][8])
        self.videosAddedToPlaylists = str(response_total["rows"][0][9])
        self.subscribersGained = str(response_total["rows"][0][10])
        self.subscribersLost = str(response_total["rows"][0][11])
        self.estimatedMinutesWatched = str(response_total["rows"][0][12])
        self.cardClicks = str(response_total["rows"][0][13])
        self.engagementRate = ( float( self.likes) + float(self.shares) + float(self.comments) + float(self.dislikes) + float(self.videosAddedToPlaylists) ) / float(self.views)
        return response_total["rows"][0]

    def getToken(self):
        print "Gettin token ========>"
        r = requests.post("https://accounts.google.com/o/oauth2/token",
            data={'client_id': Video.client_id, 'client_secret': Video.client_secret,
            'refresh_token': self.refreshToken , 'grant_type' : 'refresh_token'})
        self.accessToken = r.json()["access_token"]

    def getTopCountry(self):
        dataurl = "ids=channel==" + self.channelID + "&start-date="+ self.startDate+"&end-date="+ self.endDate+ "&filters=video==" + self.videoid + "&access_token=" + self.accessToken
        yturl = self.apiUrl + dataurl + "&metrics=views&dimensions=country&sort=-views"
        response_country = requests.get(yturl)
        response_country = response_country.json()
        for val in response_country["rows"][:3]:
            val[1] = str(val[1])
        return response_country["rows"][:3]
        #top_country = response_country["rows"][0][0]

    def getUSData(self):
        dataurl = "ids=channel==" + self.channelID + "&start-date="+ self.startDate+"&end-date="+ self.endDate + "&access_token=" + self.accessToken
        yturl = self.apiUrl + dataurl + "&metrics=views&filters=video==" + self.videoid + ";country==US&sort=-views"
        response_us_total = requests.get(yturl)
        response_us_total = response_us_total.json()
        return response_us_total["rows"][0][0]

    def getUSprovince(self):
        dataurl = "ids=channel==" + self.channelID + "&start-date="+ self.startDate+"&end-date="+ self.endDate + "&access_token=" + self.accessToken
        yturl = self.apiUrl + dataurl + "&metrics=views&filters=video==" + self.videoid+";country==US&dimensions=province&sort=-views"
        response_us = requests.get(yturl)
        response_us = response_us.json()
        for val in response_us["rows"][:3]:
            val[1] = str(val[1])
        return response_us["rows"][:3]

    def getAgeGroup(self):
        dataurl = "ids=channel==" + self.channelID + "&start-date="+ self.startDate+"&end-date="+ self.endDate+ "&filters=video==" + self.videoid + "&access_token=" + self.accessToken
        metrics = "viewerPercentage"
        yturl = self.apiUrl + dataurl + "&metrics=" + metrics + "&dimensions=ageGroup,gender"
        response_age = requests.get(yturl)
        response_age = response_age.json()
        agr = 0
        agrval = ""
        aindex = 0
        for iindex,i in enumerate(response_age["rows"]):
            if agr < i[2]:
                agr = i[2]
                agrval = i[1]
                aindex = i
        return [ str(x) for x in aindex ]

    def getGender(self):
        dataurl = "ids=channel==" + self.channelID + "&start-date="+ self.startDate+"&end-date="+ self.endDate+ "&filters=video==" + self.videoid + "&access_token=" + self.accessToken
        metrics = "viewerPercentage"
        yturl = self.apiUrl + dataurl + "&metrics=" + metrics + "&dimensions=gender"
        response_gender = requests.get(yturl)
        response_gender = response_gender.json()
        if "rows" in response_gender:
            for val in response_gender["rows"]:
                val[1] = str(val[1])
            return [ " : ".join(response_gender["rows"][0]),  " : ".join(response_gender["rows"][1])]
        else:
            return []

    def getVideoDetails(self):
        yt_data_url = "https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails&id="+ self.videoid + "&access_token=" + self.accessToken
        response_data = requests.get(yt_data_url)
        response_data = response_data.json()
        if len(response_data["items"]) == 0:
            return 0
        self.title = response_data["items"][0]["snippet"]["title"]
        self.channelTitle =  response_data["items"][0]["snippet"]["channelTitle"]
        self.published = response_data["items"][0]["snippet"]["publishedAt"]

    def getTrafficSource(self):
        metrics = "views"
        dataurl = "ids=channel==" + self.channelID + "&start-date="+ self.startDate+"&end-date="+ self.endDate+ "&filters=video==" + self.videoid + "&access_token=" + self.accessToken
        yturl = self.apiUrl + dataurl + "&metrics=" + metrics + "&dimensions=insightTrafficSourceType&sort=-views"
        response_source = requests.get(yturl)
        self.paidviews = 0
        for valAd in response_source.json()["rows"]:
            if valAd[0] == "ADVERTISING":
                self.paidviews = valAd[1]
        self.organicviews = float(self.views) - self.paidviews

    def getChannelVideos(self):
        yt_data_url = "https://www.googleapis.com/youtube/v3/search?order=date&part=snippet&channelId=" + self.channelID + "&maxResults=25" + "&access_token=" + self.accessToken
        response_data = requests.get(yt_data_url)
        print response_data.json()

if __name__ == "__main__":

    now = datetime.now()
    one_day_ago = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    ad_client_id = "xxx"
    ad_client_secret = "xxx"
    ad_refresh_token = "xxx"
    developer_token = "xxx"
    client_customer_id = "xx"

    """
    Dyanomo DB table access details 
    """

    dynamodb = boto3.resource('dynamodb')
    videoObj = dynamodb.Table('Videos')
    channelObj = dynamodb.Table('Channel')
    dataObj = dynamodb.Table('VideoData')
    response_channel = channelObj.scan()

    """
    Analytics video API calls to youtube via thread and saving the item in
    videodata table in dyanomodb
    """

    def worker():
    while True:
        item = q.get()
        print 'processing ' + str(item.videoid) + '...\n'
        item.getBasicData()
        item.getTrafficSource()
        item_json['US'] = obj.getUSprovince()
        item_json['USTotal'] = str(obj.getUSData())
        item_json['age'] = obj.getAgeGroup()
        item_json['gender'] = obj.getGender()
        item_json['likes'] = obj.likes
        item_json['views'] = obj.views
        item_json['shares'] = obj.shares
        item_json['comments'] = obj.comments
        item_json['dislikes'] = obj.dislikes
        item_json['cardClicks'] = obj.cardClicks
        item_json['engagementRate'] = str(obj.engagementRate * 100)
        item_json['annotationClickThroughRate'] = obj.annotationClickThroughRate
        item_json['annotationClicks'] = obj.annotationClicks
        item_json['estimatedMinutesWatched'] = obj.estimatedMinutesWatched
        item_json['subscribersLost'] = obj.subscribersLost
        item_json['subscribersGained'] = obj.subscribersGained
        item_json['videosAddedToPlaylists'] = obj.videosAddedToPlaylists
        item_json['averageViewDuration'] = obj.averageViewDuration
        item_json['averageViewPercentage']  = obj.averageViewPercentage
        item_json['paidviews'] = str(obj.paidviews)
        item_json['organicviews'] = str(obj.organicviews)
        item_json['country'] = obj.getTopCountry()
        item_json['videoid'] = videoid
        item_json['channelid'] = channelid
        item_json['title'] = obj.title
        item_json['channelTitle'] = obj.channelTitle
        item_yt['videoid'] = videoid
        item_yt["createDate"] = datetime.now().strftime("%Y-%m-%d")
        item_yt['ChannelID'] = channelItem["id"]
        item_yt['data'] = item_json
        dataObj.put_item(Item = item_yt)
        q.task_done()
            
    q = Queue.Queue()
    for i in range(5):
        t = threading.Thread(target=worker)
        t.daemon = True
        t.start()

    """
    Looping youtube channels and video items in dyanamodb 
    """

    for channelItem in response_channel["Items"]:
        print "Accessing dynamodb",
        refresh_token = channelItem["YT-Token"]
        channelid = channelItem["YT-ChannelID"]
        obj = Video(channelid,"https://www.googleapis.com/youtube/analytics/v1/reports?",
                        " ", one_day_ago, refresh_token, " " )

        obj.getToken()
        response_dist = videoObj.scan(
            FilterExpression=Attr('type').eq("YT") & Attr('ChannelID').eq(channelItem["id"]))
        delta = 0
        for videoItem in response_dist["Items"]:
            print videoItem["videoid"]
            videoid = videoItem["videoid"]
            obj.startDate = (datetime.strptime( videoItem["createDate"],"%Y-%m-%d") - timedelta(days=20)).strftime("%Y-%m-%d")
            obj.videoid = videoItem["videoid"]        
            if obj.getVideoDetails() == 0:
                continue
            q.put(obj)


    q.join()
    quit()


