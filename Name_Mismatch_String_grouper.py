#!/usr/bin/env python
# coding: utf-8

# In[2]:


#using stringgrouper
import pandas as pd
from string_grouper import match_strings, match_most_similar,group_similar_strings, compute_pairwise_similarities,StringGrouper


# In[5]:


df_master = pd.read_csv('Master_Affiliate.csv', error_bad_lines=False, encoding ='unicode_escape')


# In[7]:


df_actual = pd.read_csv('Actual_Affiliate.csv', error_bad_lines=False, encoding ='unicode_escape')


# In[18]:


df_actual_lst=df_actual.actual_affiliate_name.tolist()
df_master_lst=df_master.master_affiliate_name.tolist()


# In[15]:


df_actual_lst


# In[19]:


# Create a small set of artificial company names:
Actual = pd.Series(df_actual_lst)
Master = pd.Series(df_master_lst)
# Create all matches:
matches = match_strings(Master, Actual)
matches


# In[16]:


pd.Series(df_actual_lst)


# In[20]:


matches.to_csv('matches.csv', index=False)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




