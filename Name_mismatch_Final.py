#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org fuzzywuzzy


# In[2]:


pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org python-Levenshtein


# In[4]:


#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org string-grouper


# In[24]:


#from string_grouper import match_strings, match_most_similar, \
     #group_similar_strings, compute_pairwise_similarities, \
     #StringGrouper


# In[1]:


import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


# In[166]:


#creating a function to check for the fuzzy name using token_set_ratio
def checker(wrong_options,correct_options):
    names_array=[]
    ratio_array=[]    
    for wrong_option in wrong_options:
        if wrong_option in correct_options:
            names_array.append(wrong_option)
            ratio_array.append('100')
        else:   
            x=process.extractOne(wrong_option,correct_options,scorer=fuzz.ratio)
            names_array.append(x[0])
            ratio_array.append(x[1])
    return names_array,ratio_array


# In[53]:


df_Original_List=pd.read_csv('/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/Master_Affiliate.csv', error_bad_lines=False, encoding= 'unicode_escape')
                               
df_To_beMatched=pd.read_csv('/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/Actual_Affiliate.csv', error_bad_lines=False, encoding= 'unicode_escape')


# In[81]:


df_Original_List_M=pd.read_csv('/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/Salesforce _Affiliate.csv', error_bad_lines=False, encoding= 'unicode_escape')


# In[82]:


df_Original_List_M.head()


# In[54]:


df_Original_List.columns


# In[55]:


##Cleaning the columns. The main runtime errors are created by NAN Values in the column
str2Match = df_To_beMatched.actual_affiliate_name.fillna('######').tolist()
strOptions =df_Original_List.master_affiliate_name.fillna('######').tolist()


# In[57]:


name_match,ratio_match=checker(str2Match,strOptions)
df1 = pd.DataFrame()
df1['old_names']=pd.Series(str2Match)
df1['correct_names']=pd.Series(name_match)
df1['correct_ratio']=pd.Series(ratio_match)
df1.to_csv('matched_names.csv', index=False)


# In[58]:


df1.head()


# In[62]:


#Testing with real sample data
df_Original_List1=pd.read_csv(r'/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/Affiliate_name.csv', error_bad_lines=False)
                               
df_To_beMatched1=pd.read_csv(r'/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/referrel_text.csv', error_bad_lines=False)


# In[65]:


df_Original_List1.head()


# In[66]:


##Cleaning the columns. The main runtime errors are created by NAN Values in the column
str2Match1 = df_To_beMatched1.referral_text.fillna('######').tolist()
strOptions1 =df_Original_List1.affiliate_name.fillna('######').tolist()


# In[75]:


str2Match1


# In[77]:


name_match,ratio_match=checker(str2Match1,strOptions1)
df2 = pd.DataFrame()
df2['old_names']=pd.Series(str2Match1)
df2['correct_names']=pd.Series(name_match)
df2['correct_ratio']=pd.Series(ratio_match)
#df1.to_csv('matched_names.csv', index=False)


# In[78]:


df2.head()


# In[79]:


df2.to_csv('matched_names_n.csv', index=False)


# In[102]:


#Converting the Object datatype to Int datataype
df2['correct_ratio']=df2['correct_ratio'].astype(str).astype(int)


# In[103]:


df2.dtypes


# In[104]:


#Filtering all data with correct ratio >70
df2[df2['correct_ratio']>=70]


# In[ ]:


strOptionsM=pd.read_csv(r'/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/Salesforce _Affiliate.csv', error_bad_lines=False)


# In[140]:


##Cleaning the columns. The main runtime errors are created by NAN Values in the column
str2Match1 = df_To_beMatched1.referral_text.fillna('######').tolist()
strOptionsM =df_Original_List_M.Account_Name.fillna('######').tolist()


# In[84]:


strOptionsM


# In[142]:


name_match,ratio_match=checker(str2Match1,strOptionsM)
df3 = pd.DataFrame()
df3['old_names']=pd.Series(str2Match1)
df3['correct_names']=pd.Series(name_match)
df3['correct_ratio']=pd.Series(ratio_match)


# In[143]:


df3.to_csv('matched_names_sf.csv', index=False)


# In[150]:


df_strOptionsMC=pd.read_csv(r'/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/sf_affiliate_data.csv', error_bad_lines=False)


# In[146]:


strOptionsMC.dtypes


# In[86]:


#Creating a concatenated column 'Name'
df_Original_List_M['Name']= df_Original_List_M['FirstName'].map(str) + ' ' + df_Original_List_M['LastName'].map(str)


# In[147]:


#Converting the Object datatype to Int datataype
df3['correct_ratio']=df3['correct_ratio'].astype(str).astype(int)


# In[148]:


#Filtering all data with correct ratio <=83
df4 = df3[df3['correct_ratio']<=83]


# In[154]:


df4.head()


# In[152]:


##Cleaning the columns. The main runtime errors are created by NAN Values in the column for the Name column in master df
str2Match2 = df4.old_names.fillna('######').tolist()
strOptionsMC =df_strOptionsMC.contact_name .fillna('######').tolist()


# In[119]:


name_match,ratio_match=checker(str2Match2,strOptionsMC)
df5 = pd.DataFrame()
df5['old_names']=pd.Series(str2Match2)
df5['correct_names']=pd.Series(name_match)
df5['correct_ratio']=pd.Series(ratio_match)


# In[211]:


df.head()


# In[121]:


df5.to_csv('matchedcontact_names_sf.csv', index=False)


# In[214]:


df5.rename(columns = {'correct_names':'contact_name'},inplace=True)


# In[215]:


df5.head()


# In[210]:


dfA =df_strOptionsMC.copy()
dfA =df_strOptionsMC.filter(['affiliate_name', 'contact_name'], axis=1)
dfA.head()

#df_92_new = pd.concat([df5.reset_index(drop=True)\
                            #,dfA.reset_index(drop=True)], axis=0)
#df_92_new.head()


# In[216]:



df5.merge(dfA, on='contact_name', how='inner')


# In[159]:


Str1="Shipmycar"
Str2="Ship My Car"
Ratio = fuzz.ratio(Str1.lower(),Str2.lower())
Partial_ratio = fuzz.partial_ratio(Str1.lower(),Str2.lower())
Token_Sort_Ratio = fuzz.token_sort_ratio(Str1,Str2)
Token_Set_Ratio = fuzz.token_set_ratio(Str1,Str2)
print(Ratio)
print(Partial_ratio)
print(Token_Sort_Ratio)
print(Token_Set_Ratio)


# In[156]:


Str1="n"
Str2="ÃƒÂ–N"
Ratio = fuzz.ratio(Str1.lower(),Str2.lower())
Partial_ratio = fuzz.partial_ratio(Str1.lower(),Str2.lower())
Token_Sort_Ratio = fuzz.token_sort_ratio(Str1,Str2)
Token_Set_Ratio = fuzz.token_set_ratio(Str1,Str2)
print(Ratio)
print(Partial_ratio)
print(Token_Sort_Ratio)
print(Token_Set_Ratio)


# In[160]:


#testing with Fuzz.ratio with >90 ratio

#Filtering all data with correct ratio >=90
df6 = df3[df3['correct_ratio']>=90]


# In[163]:


df6.head()


# In[164]:


##Cleaning the columns. The main runtime errors are created by NAN Values in the column
str2Matchdf6 = df6.old_names.fillna('######').tolist()
strOptionsM =df_Original_List_M.Account_Name.fillna('######').tolist()


# In[167]:


name_match,ratio_match=checker(str2Matchdf6,strOptionsM)
df7 = pd.DataFrame()
df7['old_names']=pd.Series(str2Matchdf6)
df7['correct_names']=pd.Series(name_match)
df7['correct_ratio']=pd.Series(ratio_match)


# In[168]:


df7.to_csv('matched_names_sf_FR>90.csv', index=False)


# In[169]:


#creating a function to check for the fuzzy name using token_set_ratio
def checkerTSR(wrong_options,correct_options):
    names_array=[]
    ratio_array=[]    
    for wrong_option in wrong_options:
        if wrong_option in correct_options:
            names_array.append(wrong_option)
            ratio_array.append('100')
        else:   
            x=process.extractOne(wrong_option,correct_options,scorer=fuzz.token_sort_ratio)
            names_array.append(x[0])
            ratio_array.append(x[1])
    return names_array,ratio_array


# In[170]:


#Filtering all data with correct ratio <=83
df7 = df3[df3['correct_ratio']<=83]


# In[171]:


df7.head()


# In[172]:


##Cleaning the columns. The main runtime errors are created by NAN Values in the column
str2Matchdf7 = df7.old_names.fillna('######').tolist()
strOptionsMC =df_strOptionsMC.contact_name .fillna('######').tolist()


# In[174]:


name_match,ratio_match=checkerTSR(str2Matchdf7,strOptionsMC)
df8 = pd.DataFrame()
df8['old_names']=pd.Series(str2Matchdf7)
df8['correct_names']=pd.Series(name_match)
df8['correct_ratio']=pd.Series(ratio_match)


# In[175]:


df8.head()


# In[176]:


df8.to_csv('matched_names_sf_TR83CN.csv', index=False)


# In[177]:


## Testing with records having Fuzzy token sort ratio >80 to determine the correct threshold
df_to_match80=pd.read_csv('/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/matched_names_sf_new.csv', error_bad_lines=False)


# In[178]:


#Filtering all data with correct ratio >=80
df = df_to_match80[df_to_match80['correct_ratio']>=80]


# In[183]:


df.head()


# In[179]:


#creating a function to check for the fuzzy name using token_set_ratio
def checkerFL(wrong_options,correct_options):
    names_array=[]
    ratio_array=[]
    ratio_array1=[] 
    for wrong_option in wrong_options:
        if wrong_option in correct_options:
            names_array.append(wrong_option)
            ratio_array.append('100')
            ratio_array1.append('100')
        else:   
            x=process.extractOne(wrong_option,correct_options,scorer=fuzz.token_sort_ratio)
            y=process.extractOne(wrong_option,correct_options,scorer=fuzz.ratio)
            names_array.append(x[0])
            ratio_array.append(x[1])
            ratio_array1.append(y[1])
    return names_array,ratio_array,ratio_array1


# In[184]:


##Cleaning the columns. The main runtime errors are created by NAN Values in the column: doing for same output of Lazzy algo
str2Match = df.old_names.fillna('######').tolist()
strOptionsM =df.correct_names.fillna('######').tolist()


# In[186]:


name_match,ratio_match,ratio_match1=checkerFL(str2Match,strOptionsM)
df8 = pd.DataFrame()
df8['old_names']=pd.Series(str2Match)
df8['correct_names']=pd.Series(name_match)
df8['Fuzzy_Token_sort_ratio']=pd.Series(ratio_match)
df8['Levenshtein_ratio']=pd.Series(ratio_match1)


# In[187]:


df8.head()


# ### Creation of Final code for Fuzzy Name Matching

# In[189]:


strOptionsM=pd.read_csv(r'/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/Salesforce _Affiliate.csv', error_bad_lines=False)
df_To_beMatched=pd.read_csv(r'/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/referrel_text.csv', error_bad_lines=False)


# In[190]:


##Cleaning the columns. The main runtime errors are created by NAN Values in the column
str2Match = df_To_beMatched.referral_text.fillna('######').tolist()
strOptionsM =df_Original_List_M.Account_Name.fillna('######').tolist()


# In[192]:


#creating a function to check for the fuzzy name using token_set_ratio and Lev distance
def checkerFL(wrong_options,correct_options):
    names_array=[]
    names_array1=[]
    ratio_array=[]
    ratio_array1=[] 
    for wrong_option in wrong_options:
        if wrong_option in correct_options:
            names_array.append(wrong_option)
            names_array1.append(wrong_option)
            ratio_array.append('100')
            ratio_array1.append('100')
        else:   
            x=process.extractOne(wrong_option,correct_options,scorer=fuzz.token_sort_ratio)
            y=process.extractOne(wrong_option,correct_options,scorer=fuzz.ratio)
            names_array.append(x[0])
            names_array1.append(y[0])
            ratio_array.append(x[1])
            ratio_array1.append(y[1])
    return names_array,names_array1,ratio_array,ratio_array1


# In[ ]:


name_match,name_match1,ratio_match,ratio_match1=checkerFL(str2Match,strOptionsM)
df = pd.DataFrame()
df['old_names']=pd.Series(str2Match)
df['affiliate_name_Fuzzy']=pd.Series(name_match)
df['affiliate_name_Levenshtein']=pd.Series(name_match1)
df['Fuzzy_Token_sort_ratio']=pd.Series(ratio_match)
df['Levenshtein_ratio']=pd.Series(ratio_match1)


# In[ ]:


#Taking data with Fuzzy_Token_sort_ratio >=92 or Levenshtein_ratio >=92 to take the corresponding Affilate name in the microservice
df_F92 =df[df['Fuzzy_Token_sort_ratio']>=92] 
df_L92=df[df['Levenshtein_ratio']>=92]

df_92 = pd.concat([df_F92.reset_index(drop=True)                            ,df_L92.reset_index(drop=True)], axis=0)

#df_92N=df8[((df8['Fuzzy_Token_sort_ratio']>=92) | (df8['Levenshtein_ratio']>=92))]


# In[ ]:


#Exporting the Affiliate name for correct ratio >=92
df_92.to_csv('df_92.csv', index=False)


# In[ ]:


#Selecting all data that are less than equal to 91 for both Fuzzy_Token_sort_ratio and Levenshtein_ratio
df_TRL91= df[df['Fuzzy_Token_sort_ratio']<=91] 
#df_LRL91=df8[df8['Levenshtein_ratio']<=91]

df_L91 = df_TRL91[df_TRL91['Levenshtein_ratio']<=91] 
#df_LF91 = df_LRL91[df_LRL91['Fuzzy_Token_sort_ratio']<=91]


# In[ ]:


df_strOptionsMC=pd.read_csv(r'/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/sf_affiliate_data.csv', error_bad_lines=False)


# In[ ]:


str2MatchCN = df_L91.old_names.fillna('######').tolist()
strOptionsMC =df_strOptionsMC.contact_name.fillna('######').tolist()


# In[ ]:


name_match,name_match1,ratio_match,ratio_match1=checkerFL(str2MatchCN,strOptionsMC)
dfCN = pd.DataFrame()
dfCN['old_names']=pd.Series(str2MatchCN)
dfCN['contact_name_Fuzzy']=pd.Series(name_match)
dfCN['contact_name_Levenshtein']=pd.Series(name_match1)
dfCN['Fuzzy_Token_sort_ratio']=pd.Series(ratio_match)
dfCN['Levenshtein_ratio']=pd.Series(ratio_match1)


# In[ ]:


#Taking data with Fuzzy_Token_sort_ratio >=92 or Levenshtein_ratio >=92 to take the corresponding Affilate name in the microservice
dfCN_FT92 =dfCN[dfCN['Fuzzy_Token_sort_ratio']>=92] 
dfCN_LT92=dfCN[dfCN['Levenshtein_ratio']>=92]


# In[ ]:


#Creating new dataframe from Master data with only affiliate_name and contact_name

#dfA =df_strOptionsMC.copy()
#dfA =df_strOptionsMC.filter(['affiliate_name', 'contact_name'], axis=1)
dfB = df_strOptionsMC[['affiliate_name', 'contact_name']]


#Merging with Fuzzy_ratio>92 ratio dataset to get the Affilate name from Master
dfCN_FT92F=pd.merge(dfCN_FT92, dfB, left_on='contact_name_Fuzzy', right_on='contact_name', how='inner')

#Merging with Levenshtein_ratio>92 ratio dataset to get the Affilate name from Master
dfCN_L92F=pd.merge(dfCN_LT92, dfB, left_on='contact_name_Levenshtein', right_on='contact_name', how='inner')

dfCN_F92F = pd.concat([dfCN_FT92F.reset_index(drop=True)                            ,dfCN_L92F.reset_index(drop=True)], axis=0)


# In[218]:


#Exporting the Affiliate name for correct ratio >=92 with Affiliate name
dfCN_F92F.to_csv('dfCN_F92F.csv', index=False)


# In[ ]:


#Selecting all data that are less than equal to 91 for both Fuzzy_Token_sort_ratio and Levenshtein_ratio
dfCN_TRL91= dfCN[dfCN['Fuzzy_Token_sort_ratio']<=91] 
#df_LRL91=df8[df8['Levenshtein_ratio']<=91]

dfCN_L91 = dfCN_TRL91[dfCN_TRL91['Levenshtein_ratio']<=91] 
#df_LF91 = df_LRL91[df_LRL91['Fuzzy_Token_sort_ratio']<=91]

##Assigning 'Null' Affiliate Name 
dfCN_L91['affiliate_name']=""

