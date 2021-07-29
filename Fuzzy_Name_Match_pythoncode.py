#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org fuzzywuzzy
#pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org python-Levenshtein

##### 
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

strOptionsM=pd.read_csv(r'/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/Salesforce _Affiliate.csv', error_bad_lines=False)
df_To_beMatched=pd.read_csv(r'/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/referrel_text.csv', error_bad_lines=False)


##Cleaning the columns. The main runtime errors are created by NAN Values in the column
str2Match = df_To_beMatched.referral_text.fillna('######').tolist()
strOptionsM =df_Original_List_M.Account_Name.fillna('######').tolist()

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
    
 
name_match,name_match1,ratio_match,ratio_match1=checkerFL(str2Match,strOptionsM)
df = pd.DataFrame()
df['old_names']=pd.Series(str2Match)
df['affiliate_name_Fuzzy']=pd.Series(name_match)
df['affiliate_name_Levenshtein']=pd.Series(name_match1)
df['Fuzzy_Token_sort_ratio']=pd.Series(ratio_match)
df['Levenshtein_ratio']=pd.Series(ratio_match1)

#Taking data with Fuzzy_Token_sort_ratio >=92 or Levenshtein_ratio >=92 to take the corresponding Affilate name in the microservice
df_F92 =df[df['Fuzzy_Token_sort_ratio']>=92] 
df_L92=df[df['Levenshtein_ratio']>=92]

df_92 = pd.concat([df_F92.reset_index(drop=True)\
                            ,df_L92.reset_index(drop=True)], axis=0)


#Exporting the Affiliate name for correct ratio >=92
df_92.to_csv('df_92.csv', index=False)

#Selecting all data that are less than equal to 91 for both Fuzzy_Token_sort_ratio and Levenshtein_ratio
df_TRL91= df[df['Fuzzy_Token_sort_ratio']<=91] 
#df_LRL91=df8[df8['Levenshtein_ratio']<=91]

df_L91 = df_TRL91[df_TRL91['Levenshtein_ratio']<=91] 
#df_LF91 = df_LRL91[df_LRL91['Fuzzy_Token_sort_ratio']<=91]


df_strOptionsMC=pd.read_csv(r'/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/sf_affiliate_data.csv', error_bad_lines=False)


##Cleaning the columns. The main runtime errors are created by NAN Values in the column
str2MatchCN = df_L91.old_names.fillna('######').tolist()
strOptionsMC =df_strOptionsMC.contact_name.fillna('######').tolist()


name_match,name_match1,ratio_match,ratio_match1=checkerFL(str2MatchCN,strOptionsMC)
dfCN = pd.DataFrame()
dfCN['old_names']=pd.Series(str2MatchCN)
dfCN['contact_name_Fuzzy']=pd.Series(name_match)
dfCN['contact_name_Levenshtein']=pd.Series(name_match1)
dfCN['Fuzzy_Token_sort_ratio']=pd.Series(ratio_match)
dfCN['Levenshtein_ratio']=pd.Series(ratio_match1)


#Taking data with Fuzzy_Token_sort_ratio >=92 or Levenshtein_ratio >=92 to take the corresponding Affilate name in the microservice
dfCN_FT92 =dfCN[dfCN['Fuzzy_Token_sort_ratio']>=92] 
dfCN_LT92=dfCN[dfCN['Levenshtein_ratio']>=92]

#Creating new dataframe from Master data with only affiliate_name and contact_name

#dfA =df_strOptionsMC.copy()
#dfA =df_strOptionsMC.filter(['affiliate_name', 'contact_name'], axis=1)
dfB = df_strOptionsMC[['affiliate_name', 'contact_name']]


#Merging with Fuzzy_ratio>92 ratio dataset to get the Affilate name from Master
dfCN_FT92F=pd.merge(dfCN_FT92, dfB, left_on='contact_name_Fuzzy', right_on='contact_name', how='inner')

#Merging with Levenshtein_ratio>92 ratio dataset to get the Affilate name from Master
dfCN_L92F=pd.merge(dfCN_LT92, dfB, left_on='contact_name_Levenshtein', right_on='contact_name', how='inner')

dfCN_F92F = pd.concat([dfCN_FT92F.reset_index(drop=True)\
                            ,dfCN_L92F.reset_index(drop=True)], axis=0)


#Exporting the Affiliate name for correct ratio >=92 with Affiliate name
dfCN_F92F.to_csv('dfCN_F92F.csv', index=False)

#Selecting all data that are less than equal to 91 for both Fuzzy_Token_sort_ratio and Levenshtein_ratio
dfCN_TRL91= dfCN[dfCN['Fuzzy_Token_sort_ratio']<=91] 
#df_LRL91=df8[df8['Levenshtein_ratio']<=91]

dfCN_L91 = dfCN_TRL91[dfCN_TRL91['Levenshtein_ratio']<=91] 
#df_LF91 = df_LRL91[df_LRL91['Fuzzy_Token_sort_ratio']<=91]

##Assigning 'Null' Affiliate Name 
dfCN_L91['affiliate_name']=""

#Exporting the Affiliate name for correct ratio <91 with NULL  Affiliate name
dfCN_L91.to_csv('dfCN_L91.csv', index=False)


