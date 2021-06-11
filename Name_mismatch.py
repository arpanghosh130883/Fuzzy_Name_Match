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


# In[25]:


#creating a function to check for the fuzzy name using token_set_ratio
def checker(wrong_options,correct_options):
    names_array=[]
    ratio_array=[]    
    for wrong_option in wrong_options:
        if wrong_option in correct_options:
            names_array.append(wrong_option)
            ratio_array.append('100')
        else:   
            x=process.extractOne(wrong_option,correct_options,scorer=fuzz.token_set_ratio)
            names_array.append(x[0])
            ratio_array.append(x[1])
    return names_array,ratio_array


# In[53]:


df_Original_List=pd.read_csv('/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/Master_Affiliate.csv', error_bad_lines=False, encoding= 'unicode_escape')
                               
df_To_beMatched=pd.read_csv('/home/mvisi/Project/DLP/Core/FraudPredict/Notebook/Arpan/Actual_Affiliate.csv', error_bad_lines=False, encoding= 'unicode_escape')


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


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




