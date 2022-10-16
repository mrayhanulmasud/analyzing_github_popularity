#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  5 02:39:13 2021

@author: masud
"""
import time 
import pandas as pd
import os
import numpy as np
import networkx as nx
from os.path import exists
import warnings
warnings.filterwarnings("ignore")

ITERATION_COUNT=100
MAX_VAL_SCORE=100

DATASET_FILENAME='dataset.csv'
dataset_directory='Dataset'
follower_directory=dataset_directory + '/Followers'
stargazer_directory=dataset_directory + '/Stargazers'

attribute_info_1=dataset_directory + '/all_mal_repo_attribute_info_1.csv'
attribute_info_2=dataset_directory + '/all_mal_repo_attribute_info_2.csv'
is_org_info_3=dataset_directory + '/is_org_info.csv'
email_is_hireable_stars_info_4=dataset_directory + '/owner_email_is_hireable_stars.csv'
blog_info_5=dataset_directory + '/blog_info.csv'


def get_malware_authors_having_followers():
    
    malware_authors_having_followers=[]
    
    # iterate over files in that directory to list the malware authors
    for root, dirs, files in os.walk(follower_directory):
        for filename in files:
            if '.csv' in filename:
                author_login_name= filename.replace('_followers.csv', '')
                malware_authors_having_followers.append(author_login_name)
                
    print('Malware Authors Having Followers', len(malware_authors_having_followers))
    
    return malware_authors_having_followers

def get_malware_authors_having_mutual_followers(malware_authors_having_followers):
    
    directed_edge_list=[]
    node_list=set()
    
    for root, dirs, files in os.walk(follower_directory):
        for filename in files:
            if '.csv' in filename:
                author_login_name = filename.replace('_followers.csv', '')
                file_path = os.path.join(root, filename)
                
                with open(file_path, 'r') as fp:
                    followers_info=fp.readlines()

                    for follower_info in followers_info:
                        follower_author_login_name = follower_info.split(',')[0]
                        
                        
                        if follower_author_login_name in malware_authors_having_followers:
                            
                            directed_edge_list.append((follower_author_login_name, 
                                                       author_login_name))
                            node_list.add(follower_author_login_name)
                            node_list.add(author_login_name)
    
    print('Malware Authors Having Mutual Followers', len(node_list))
    print('Edge Count', len(directed_edge_list))
    
    return directed_edge_list, list(node_list)
        

def get_blog_given_authors():
        
    df_5=pd.read_csv(blog_info_5, names=['login_name', 'blog_url'])
    
    df_5['blog_given']=(~df_5['blog_url'].isnull()).astype(bool)
    df_5['blog_given'].replace([True, False], [1,0], inplace=True)
    
    df_5=df_5[df_5['blog_given'] == 1]
    blog_given_authors=df_5['login_name'].unique()
    
    # print(df_5.shape)
    # print(len(blog_given_authors))
    
    return blog_given_authors

def get_email_given_authors():
    
    df_4=pd.read_csv(email_is_hireable_stars_info_4)
    df_4['login_name']=df_4['full_name'].apply(lambda full_name: full_name.split('/')[0]).astype(str)
    
    df_4['email_given']=(~df_4['owner_email'].isnull()).astype(bool)
    df_4['email_given'].replace([True, False], [1,0], inplace=True)
    

    df_4=df_4[df_4['email_given'] == 1]
    email_given_authors=df_4['login_name'].unique()
    
    # print(df_4.shape)
    # print(len(email_given_authors))
    
    return email_given_authors

def get_is_hireable_authors():
    
    df_4=pd.read_csv(email_is_hireable_stars_info_4)
    df_4['login_name']=df_4['full_name'].apply(lambda full_name: full_name.split('/')[0]).astype(str)

    df_4=df_4[df_4['owner_is_hireable'] == True]
    is_hireable_authors=df_4['login_name'].unique()
    
    # print(df_4.shape)
    # print(len(is_hireable_authors))
    
    return is_hireable_authors

def get_filtered_repo_data(mutually_following_authors, repo_star_map):
    
    df_all_repos_1=pd.read_csv(attribute_info_1)
    # print(df_all_repos_1.columns)
    print('Num of Repos from file 1', df_all_repos_1.shape[0])
    
    
    df_all_repos_2=pd.read_csv(attribute_info_2)
    # print(df_all_repos_2.columns)
    print('Num of Repos from file 2', df_all_repos_2.shape[0])
    
    ## Merging all files
    df_all_repos_merged=pd.concat([df_all_repos_1,df_all_repos_2], ignore_index=True)
    # print(df_merged.columns)
    print('Total Num of Repos', df_all_repos_merged.shape[0])
    
    df_all_repos_merged['login_name']=df_all_repos_merged['full_name'].apply(lambda full_name: full_name.split('/')[0]).astype(str)
    # print(df_all_repos_merged[['full_name', 'login_name']])
    
    
    ## Filter the repos of the multually following authors
    df_all_repos_merged_filtered=df_all_repos_merged[
                                df_all_repos_merged['login_name'].isin(mutually_following_authors)]
    
    
    ###############
    with open(stargazer_directory + '/failed_final.txt', 'r') as sd:
        repos_not_found=sd.readlines()
        for repo_not_found in repos_not_found:
            repo_not_found=repo_not_found.strip()
        print('Repo not found for ', len(repos_not_found))
    
    print('Before ignoring not found repo', df_all_repos_merged_filtered.shape)
    df_all_repos_merged_filtered=df_all_repos_merged_filtered[
                                ~df_all_repos_merged_filtered['full_name'].isin(repos_not_found)]
    print('After ignoring not found repo', df_all_repos_merged_filtered.shape)
    
    for repo_name in repo_star_map:
        
        indices=df_all_repos_merged_filtered[df_all_repos_merged_filtered['full_name'] == repo_name].index
        df_all_repos_merged_filtered.loc[indices[0],
            ['stargazers_count'] ] = [ repo_star_map[repo_name] ]
    
    #################
        
    # #########
    # df_all_repos_merged_filtered['created_at_ch'] = pd.to_datetime(df_all_repos_merged_filtered['created_at'])
    
    # df_all_repos_merged_filtered['updated_at_ch'] = pd.Timestamp(year=2021, month=11, day=1)
    
    # df_all_repos_merged_filtered['duration'] = (
    #     df_all_repos_merged_filtered['updated_at_ch'] - df_all_repos_merged_filtered['created_at_ch']).astype('timedelta64[D]')
    
    # df_all_repos_merged_filtered['duration']=df_all_repos_merged_filtered['duration']/30
    # print(df_all_repos_merged_filtered['duration'].describe())
    
    # df_all_repos_merged_filtered['stargazers_count']=(
    #     df_all_repos_merged_filtered['stargazers_count']/df_all_repos_merged_filtered['duration']
    #     ).astype(float)
    
    # df_all_repos_merged_filtered['forks_count']=(
    #     df_all_repos_merged_filtered['forks_count']/df_all_repos_merged_filtered['duration']
    #     ).astype(float)
    
    # df_all_repos_merged_filtered['subscribers_count']=(
    #     df_all_repos_merged_filtered['subscribers_count']/df_all_repos_merged_filtered['duration']
    #     ).astype(float)
    
    # # ########
    
    ## Filter the repos of author who have twitter
    df_all_repos_merged_filtered['twitter_given']=(~df_all_repos_merged_filtered['owner_twitter_username'].isnull()).astype(bool)
    df_all_repos_merged_filtered['twitter_given'].replace([True, False], [1,0], inplace=True)
    # print(df_all_repos_merged_filtered[['owner_twitter_username', 'twitter_given']])
    
    ## Filter the repos of author who have twitter
    df_all_repos_merged_filtered['location_given']=(~df_all_repos_merged_filtered['owner_location'].isnull()).astype(bool)
    df_all_repos_merged_filtered['location_given'].replace([True, False], [1,0], inplace=True)
    # print(df_all_repos_merged_filtered[['owner_location', 'location_given']])
    
    
    df_all_repos_merged_filtered=df_all_repos_merged_filtered[
            ['login_name', 'twitter_given', 'location_given', 'owner_followers',
             'owner_following',  'stargazers_count', 
             'forks_count', 'subscribers_count',
             'owner_twitter_username', 'owner_location'] ]
    
    df_all_repos_merged_filtered=df_all_repos_merged_filtered.groupby(
        ['login_name', 'twitter_given',
         'location_given', 'owner_followers', 'owner_following',
         'owner_twitter_username', 'owner_location'], 
            as_index=False,
            dropna=False).agg(
                star_max=pd.NamedAgg(column="stargazers_count", aggfunc="max"),
                fork_max=pd.NamedAgg(column="forks_count", aggfunc="max"),
                subscriber_max=pd.NamedAgg(column="subscribers_count", aggfunc="max"))
    
    # print(df_all_repos_merged_filtered.columns)
    # print(df_all_repos_merged_filtered[['login_name', 'twitter_given', 'star_max']])
    print('Total Num of Repos after filtering', df_all_repos_merged_filtered.shape[0])
    print('Total Num of columns after filtering', df_all_repos_merged_filtered.shape[1])
    
    
    df_all_repos_merged_filtered=df_all_repos_merged_filtered[
        df_all_repos_merged_filtered['login_name'].duplicated() == False]
    print('Total Num of Repos after deduplicated', df_all_repos_merged_filtered.shape[0])
    
    
    return df_all_repos_merged_filtered


def add_column(info_given, authors_given, df):
    
    df[info_given]=0
    df.loc[df['login_name'].isin(authors_given), info_given]=1

def run_hits(directed_edge_list, malware_authors, iteration_count, max_value):
    
    G = nx.DiGraph()
 
    G.add_edges_from(directed_edge_list)
    print('graph created')
    print(G.number_of_nodes())
    # plt.figure(figsize =(10, 10))
    # nx.draw_networkx(G, with_labels = False)
    # plt.show()
     
    # hubs, authorities = nx.hits(G, max_iter = iteration_count)
    hubs, authorities = nx.hits(G, max_iter = iteration_count, normalized = False)
    
    # print(min(hubs.values()))
    # print(max(hubs.values()))
    
    # print(min(authorities.values()))
    # print(max(authorities.values()))
    
    # normalize hubs/authorities
    min_hub=min(hubs.values())
    max_hub=max(hubs.values())
    
    min_auth=min(authorities.values())
    max_auth=max(authorities.values())
    
    authority_scores=[]
    hub_scores=[]
    total_conts=[]
    # zi = (xi – min(x)) / (max(x) – min(x)) * 100 = (12 – 12) / (68 – 12) * 100 = 0
    for iter in range(0, len(malware_authors)):
        
        author=malware_authors[iter]
        
        hub_val = ( (hubs[author]-min_hub) / (max_hub-min_hub) ) * max_value
        # hubs[author] = "{:.2f}".format(hub_val)
        hubs[author]=int(round(hub_val, 0))
        hub_val=hubs[author]
        hub_scores.append(hubs[author])
        
        auth_val = ( (authorities[author]-min_auth) / (max_auth-min_auth) ) * max_value
        # authorities[author] = "{:.2f}".format(auth_val)
        authorities[author]=int(round(auth_val, 0))
        auth_val=authorities[author]
        authority_scores.append(authorities[author])
        
        total_conts.append( (hub_val+auth_val))
    
    # show_dis_hub_auth(hub_scores, authority_scores, total_conts)
    
    return authority_scores, hub_scores, total_conts

def add_hub_authority_scores(filtered_repo_data, mutually_following_authors,
                             authority_scores, hub_scores, total_conts) :
    
    filtered_repo_data['auth_score']=0
    filtered_repo_data['hub_score']=0
    filtered_repo_data['total_score']=0
    
    filtered_repo_data['auth_score']=filtered_repo_data['auth_score'].astype(int)
    filtered_repo_data['hub_score']=filtered_repo_data['hub_score'].astype(int)
    filtered_repo_data['total_score']=filtered_repo_data['total_score'].astype(int)
    
    for iter in range(0, len(mutually_following_authors)):
        
        author=mutually_following_authors[iter]
        indices=filtered_repo_data[filtered_repo_data['login_name'] == author].index
        
        # if len(indices) > 1:
        #     print('***********error*********')
        
        # print(indices[0])
        
        filtered_repo_data.loc[indices[0],
            ['auth_score', 'hub_score', 'total_score'] ] = [ 
                authority_scores[iter], 
                hub_scores[iter],
                total_conts[iter] ]
                
        # tmp=filtered_repo_data[filtered_repo_data['login_name'] == author] 
        # print(tmp[['auth_score', 'hub_score', 'total_score']].head())
        # print(authority_scores[iter], hub_scores[iter], total_conts[iter])
        
def add_blog_information(filtered_repo_data, mutually_following_authors):
    
    df_5=pd.read_csv(blog_info_5, names=['login_name', 'blog_url'])
    
    df_5['blog_given']=(~df_5['blog_url'].isnull())
    df_5['blog_given'].replace([True, False], [1,0], inplace=True)
    df_5['blog_given']=df_5['blog_given'].astype(int)
    
    filtered_repo_data['blog_url']=np.nan
    filtered_repo_data['blog_given']=0
    filtered_repo_data['blog_url']=filtered_repo_data['blog_url'].astype(str)
    filtered_repo_data['blog_given']=filtered_repo_data['blog_given'].astype(int)
    
    blog_info_not_found=0
    for iter in range(0, len(mutually_following_authors)):
        
        author=mutually_following_authors[iter]
        indices=filtered_repo_data[filtered_repo_data['login_name'] == author].index
        
        blog_info=df_5[df_5['login_name'] == author.lower()].head(1)
        
        if blog_info.shape[0] == 1:
            blog_info=blog_info[['blog_url', 'blog_given']].values.tolist()[0]
            filtered_repo_data.loc[indices[0],['blog_url', 'blog_given'] ] = blog_info
        else:
            blog_info_not_found=blog_info_not_found+1
        
    print('Blog information not found for', blog_info_not_found, "authors")


def add_email_is_hireable_information(filtered_repo_data, mutually_following_authors):
    
    df_4=pd.read_csv(email_is_hireable_stars_info_4)
    df_4['login_name']=df_4['full_name'].apply(lambda full_name: full_name.split('/')[0]).astype(str)
    
    df_4['email_given']=(~df_4['owner_email'].isnull())
    df_4['email_given'].replace([True, False], [1,0], inplace=True)
    df_4['email_given']=df_4['email_given'].astype(int)
    
    df_4['owner_is_hireable'].replace([True, False], [1,0], inplace=True)
    df_4['owner_is_hireable']=df_4['owner_is_hireable'].astype(int)
    
    filtered_repo_data['owner_email']=np.nan
    filtered_repo_data['email_given']=0
    filtered_repo_data['owner_is_hireable']=0
    
    filtered_repo_data['owner_email']=filtered_repo_data['owner_email'].astype(str)
    filtered_repo_data['email_given']=filtered_repo_data['email_given'].astype(int)
    filtered_repo_data['owner_is_hireable']=filtered_repo_data['owner_is_hireable'].astype(int)
    
    email_hireable_info_not_found=0
    for iter in range(0, len(mutually_following_authors)):
        
        author=mutually_following_authors[iter]
        indices=filtered_repo_data[filtered_repo_data['login_name'] == author].index
        
        email_hireable_info=df_4[df_4['login_name'] == author].head(1)
        
        if email_hireable_info.shape[0] == 1:
            email_hireable_info=email_hireable_info[
                ['owner_email', 'email_given', 'owner_is_hireable']].values.tolist()[0]
            filtered_repo_data.loc[indices[0],
                ['owner_email', 'email_given', 'owner_is_hireable'] ] = email_hireable_info
        else:
            email_hireable_info_not_found=email_hireable_info_not_found+1
    
        
    print('Email/Hireable information not found for', 
          email_hireable_info_not_found, "authors")
                

def get_star_count_mutual_followers(mutually_following_authors):
    
    repo_star_map={}
    for root, dirs, files in os.walk(stargazer_directory):
        for filename in files:
            if '.csv' in filename:
                repo_name = filename.replace('_stargazers.csv', '')
                repo_name = repo_name.replace(' ', '/', 1)
                # print(repo_name)
                
                filepath=os.path.join(root, filename)
                st_list=[]
                with open(filepath, 'r') as f:
                    stargazers=f.readlines()
                    star_sum=0
                    for stargazer in stargazers:
                        stargazer=stargazer.strip()
                        
                        if stargazer in mutually_following_authors:
                            star_sum=star_sum+1
                            st_list.append(stargazer)
                    repo_star_map[repo_name]=star_sum
                    # print(len(st_list), repo_star_map[repo_name])
                    # print(st_list)
                    # time.sleep(10)
                    # print(repo_name, star_count)
                    
    return repo_star_map
                
    
def prepare_dataset() :
    
    ## We are going to have the list of authors who have malware authors as followers
    malware_authors_having_followers=get_malware_authors_having_followers()
    
    
    ## We are filtering the mutually following authors
    directed_edge_list, mutually_following_authors=get_malware_authors_having_mutual_followers(
                malware_authors_having_followers)
    
    repo_star_map=get_star_count_mutual_followers(mutually_following_authors)
    
    ## We are extracting the repositories of mutually following authors
    filtered_repo_data=get_filtered_repo_data(mutually_following_authors, repo_star_map)
    
    ## We are adding blog information 
    add_blog_information(filtered_repo_data, mutually_following_authors)
    
    ## We are adding email/hireable information 
    add_email_is_hireable_information(filtered_repo_data, mutually_following_authors)
    
    print('Before hits', filtered_repo_data.shape)
    
    authority_scores, hub_scores, total_conts=run_hits(
        directed_edge_list, 
        mutually_following_authors, 
        ITERATION_COUNT, 
        MAX_VAL_SCORE)
    
    add_hub_authority_scores(filtered_repo_data, mutually_following_authors,
                              authority_scores, hub_scores, total_conts)
    
    print('Dataset is created with size', filtered_repo_data.shape)
    
    return filtered_repo_data
    
if __name__ == "__main__":
    
    file_exists = exists(DATASET_FILENAME)
    
    if  not file_exists:
        dataset = pd.read_csv(DATASET_FILENAME)
        print('Dataset read done')
    
    else:
        print("No Dataset found")
        
        dataset=prepare_dataset()
        dataset.to_csv(DATASET_FILENAME)
        print('Dataset write done')
    
    
    # df_all_info=pd.read_csv(attribute_info_1)    
    # df_all_info['created_at_converted'] = pd.to_datetime(df_all_info['created_at'])
    
    # # date_before = datetime.date(2019, 1, 19)
    # df_all_info['created_at_converted'] = (pd.Timestamp(year=2021, month=11, day=1) - df_all_info['created_at_converted']).astype('timedelta64[D]')
    # df_all_info['created_at_converted']=round(df_all_info['created_at_converted']/30)
    # print(df_all_info['created_at_converted'].describe())
    
    # df_all_info['stargazers_count']=round(df_all_info['stargazers_count']/df_all_info['created_at_converted'])
    
    # print(df_all_info[ ['stargazers_count', 'created_at', 'created_at_converted'] ].tail())
        
    
    
    
    
    
    
    
        



