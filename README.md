# Fundraising-Campaign-Predictive-Model
A predictive model for a Charity Organization to predict who is likely to donate, for the fundraising campaign and how much money they are likely to give. This model was created for an assignment in the Marketing Analytics course in the Data Science Master's by ESSEC-CentraleSupelec. 

The goal is to predict who is likely to make a donation to the charity for the fundraising campaign 
“C189”  (%),  and  how  much  money  they  are  likely  to  give  if  they  do  (€).  

By  combining  these  two predictions, you will obtain an expected revenue from each individual. 
Every solicitation costs 2.00 € (a fake, unrealistic figure used for the purpose of this exercise). 

If the expected revenue you have predicted exceeds that figure of 2 €, you will recommend the charity 
to solicit that individual (solicit = 1), since the expected profit is positive. If it is below 2 €, you will 
recommend the charity not to solicit that individual (solicit = 0), since on average you expect a loss. 
For the purpose of this exercise, we will assume that no individual is going to make a donation on that 
campaign if he is not directly solicited by the charity. We will also ignore all donations made under 
automatic deductions following that campaign (they will not be included in the data). 
Your objective is to maximize the financial performance of that campaign for the charity.
