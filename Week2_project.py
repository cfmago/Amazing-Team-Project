# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 11:29:36 2020

@author: gonca
"""


school_dic = {   
'ironhack' : 10828,
'app-academy' : 10525,
'springboard' : 11035    
}

import re
import pandas as pd
from pandas.io.json import json_normalize
import requests



def get_comments_school(school):
  TAG_RE = re.compile(r'<[^>]+>')
  # defines url to make api call to data -> dynamic with school if you want to scrape competition
  url = "https://www.switchup.org/chimera/v1/school-review-list?mainTemplate=school-review-list&path=%2Fbootcamps%2F" + school + "&isDataTarget=false&page=3&perPage=10000&simpleHtml=true&truncationLength=250"
  #makes get request and converts answer to json
  data = requests.get(url).json()
  #converts json to dataframe
  reviews =  pd.DataFrame(data['content']['reviews'])
  
  #aux function to apply regex and remove tags
  def remove_tags(x):
    return TAG_RE.sub('',x)
  reviews['review_body'] = reviews['body'].apply(remove_tags)
  reviews['school'] = school
  return reviews


# could you write this as a list comprehension? ;)
comments = []

for school in school_dic.keys():
    print(school)
    comments.append(get_comments_school(school))

comments = pd.concat(comments)

from pandas.io.json import json_normalize

def get_school_info(school, school_id):
    url = 'https://www.switchup.org/chimera/v1/bootcamp-data?mainTemplate=bootcamp-data%2Fdescription&path=%2Fbootcamps%2F'+ str(school) + '&isDataTarget=false&bootcampId='+ str(school_id) + '&logoTag=logo&truncationLength=250&readMoreOmission=...&readMoreText=Read%20More&readLessText=Read%20Less'

    data = requests.get(url).json()

    data.keys()

    courses = data['content']['courses']
    courses_df = pd.DataFrame(courses, columns= ['courses'])

    locations = data['content']['locations']
    locations_df = json_normalize(locations)

    badges_df = pd.DataFrame(data['content']['meritBadges'])
    
    website = data['content']['webaddr']
    description = data['content']['description']
    logoUrl = data['content']['logoUrl']
    school_df = pd.DataFrame([website,description,logoUrl]).T
    school_df.columns =  ['website','description','LogoUrl']

    locations_df['school'] = school
    courses_df['school'] = school
    badges_df['school'] = school
    school_df['school'] = school
    
    # how could you write a similar block of code to the above in order to record the school ID?

    return locations_df, courses_df, badges_df, school_df

locations_list = []
courses_list = []
badges_list = []
schools_list = []

for school, id in school_dic.items():
    a,b,c,d = get_school_info(school,id)
    locations_list.append(a)
    courses_list.append(b)
    badges_list.append(c)
    schools_list.append(d)
    
locations = pd.concat(locations_list)
courses = pd.concat(courses_list)
badges = pd.concat(badges_list)
schools = pd.concat(schools_list)

#cleaning comments
def clean_comments(comments):
    #Changes in types
    comments['overallScore'] = comments['overallScore'].astype('float')
    comments['curriculum'] = comments['curriculum'].astype('float')
    comments['jobSupport'] = comments['jobSupport'].astype('float')
    comments['id'] = comments['id'].astype('object')
    comments['queryDate'] = pd.to_datetime(comments['queryDate'])
    #Change type and clean graduating years
    def correct_year(x):
        YEAR_RE = re.compile(r'\..')
        return YEAR_RE.sub('',x)
    import datetime
    comments["graduatingYear"] = comments["graduatingYear"].astype(str).apply(correct_year)
    #Dropping unecessary columns
    drop_cols = ["user", "body", "createdAt", "comments", "hostProgramName"]   
    comments = comments.drop(drop_cols, axis=1)
    #Renaming columns
    comments.rename(columns={'review_body': 'Review'}, inplace=True)
    #Substituting nulls
    comments = comments.fillna('not available')
    return comments

#cleaning locations
def clean_locations(locations):
    locations['id'] = locations['id'].astype('object')
    locations['city.id'] = locations['city.id'].astype('object')
    locations['country.id'] = locations['country.id'].astype('object')
    locations['state.id'] = locations['state.id'].astype('object')
    locations['city.id'] = locations['city.id'].astype('object')
    locations = locations.fillna('not available')
    return locations

#cleaning badges
def clean_badge_school(badges):
    TAG_RE = re.compile(r'<[^>]+>')
    def remove_tags(x):
        return TAG_RE.sub('',x)
    badges['description'] = badges['description'].apply(remove_tags)
    return badges

#Getting new versions goinggg
comments = clean_comments(comments) 
locations = clean_locations(locations)
badges = clean_badge_school(badges)
schools = clean_badge_school(schools)


#Cretion of table with relation between scholls and id
id_df=pd.DataFrame(school_dic.items(), columns=["school","school_id"])

#Adding the id of schools to evey column
locations = locations.merge( id_df, how="inner", left_on='school', right_on='school')
comments = comments.merge( id_df, how="inner", left_on='school', right_on='school')
badges = badges.merge( id_df, how="inner", left_on='school', right_on='school')
schools = schools.merge( id_df, how="inner", left_on='school', right_on='school')
courses = courses.merge( id_df, how="inner", left_on='school', right_on='school')



print(locations,comments,badges,schools,courses)


from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root",pw="D4taR00t!",db="amazing_team"))
schools.to_sql('Schools', con = engine, if_exists = 'append', chunksize = 1000)

comments.to_sql('Comments', con = engine, if_exists = 'append', chunksize = 1000)

badges.to_sql('Badges', con = engine, if_exists = 'append', chunksize = 1000)

locations.to_sql('Location', con = engine, if_exists = 'append', chunksize = 1000)

courses.to_sql('Courses', con = engine, if_exists = 'append', chunksize = 1000)





















