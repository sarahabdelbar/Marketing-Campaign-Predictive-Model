#THIS IS IT
library(RODBC)

#Open the connection
db = odbcConnect("my_data_source",uid="root",pwd="colombia")
sqlQuery(db, "USE ma_charity_full")
query=
  "
SELECT 
as2.contact_id AS contact_id,
ifnull(as2.amount,0) AS final_amount,
ifnull(as2.donation,0) as will_donate,
ifnull(a.years_since_latest_donation,0) AS years_since_latest_donation, 
ifnull(a.years_since_first_donation_ever,0) AS years_since_first_donation_ever,  
ifnull(a.average_do_amount,0) AS average_do_amount, 
ifnull(a.num_donations,0) AS num_donations, 
ifnull(sol.num_solicitations,0) AS num_solicitations, 
ifnull(a.num_donations/sol.num_solicitations,0) AS conversion_rate, 
CASE WHEN pa.pa_donations > 0 THEN 1
	ELSE 0 END AS pa_doner, 
ifnull(a.years_since_latest_donation/((a.years_since_first_donation_ever-a.years_since_latest_donation)/a.num_donations),0) AS time_todonate_ratio,
ifnull((a.years_since_first_donation_ever-a.years_since_latest_donation)/a.num_donations,0) AS consistency,  #time_between_donations
ifnull(a.max_do_donation,0) AS max_do_donation, 
ifnull(c.max_overall_donation,0) AS max_overall_donation, 
ifnull(c.avg_overall_amount,0) AS avg_overall_amount, 
ifnull(d.max_summer_donation,0) AS max_summer_donation, 
ifnull(d.avg_summer_donations,0) AS avg_summer_donations,
ifnull(d.num_summer_donations,0) AS num_summer_donations, 
ifnull(t.may_june_donations/t.may_june_solicitations,0) as may_june_conversion,
gender.is_female as is_female,
gender.is_male as is_male, 
gender.is_gender_unkown as is_gender_unkown,
gender.is_couple as is_couple,
binaries.donated_june16 as donated_june16,
binaries.donated_june17 as donated_june17

FROM assignment2 as2
LEFT JOIN ( 
			SELECT contact_id, 
            DATEDIFF(20180625, MAX(act_date))/ 365 AS years_since_latest_donation, 
            DATEDIFF(20180625, MIN(act_date))/365 AS years_since_first_donation_ever,
			avg(amount) AS average_do_amount,
            COUNT(amount) AS num_donations,
            IFNULL(MAX(amount),0) AS max_do_donation
			FROM acts
            WHERE act_type_id= 'DO' 
			GROUP BY 1) AS a
            on a.contact_id= as2.contact_id
LEFT JOIN ( 
SELECT contact_id,
COUNT(action_date) AS num_solicitations
FROM actions
GROUP BY 1) AS sol
on sol.contact_id = as2.contact_id
LEFT JOIN (
			SELECT contact_id, 
            IFNULL(MAX(amount),0) as max_overall_donation,
            IFNULL(avg(amount),0) as avg_overall_amount
            FROM acts 
            GROUP BY 1) as c
            on c.contact_id = as2.contact_id
LEFT JOIN(
			SELECT contact_id, 
            IFNULL(MAX(amount),0) as max_summer_donation,
            IFNULL(count(act_date),0) as num_summer_donations, 
            IFNULL(AVG(amount),0) as avg_summer_donations
            FROM acts
            WHERE month(act_date) in (5,6,7,8)
            GROUP BY 1) as d
            ON d.contact_id = as2.contact_id
LEFT JOIN(
			SELECT contact_id, SUM(amount) as pa_donations
            FROM acts 
            WHERE act_type_id = 'PA'
            GROUP BY 1) as pa
            on pa.contact_id = as2.contact_id
LEFT JOIN 
(
				SELECT a.contact_id, count(action_date) as may_june_solicitations, ifnull(b.may_june_donations,0) as may_june_donations #the contact, the campaign and the number of solicitation per campaign in May June
				FROM actions a
				LEFT JOIN (
				SELECT contact_id, count(act_date) as may_june_donations
				FROM acts
				WHERE (MONTH(act_date) in (5,6) AND YEAR(act_date) in (2015,2016,2017,2018))
				GROUP BY 1) as b
				on b.contact_id= a.contact_id
				WHERE (MONTH(a.action_date) in (5,6) AND YEAR(action_date) in (2015,2016,2017,2018))
				GROUP BY 1
) t ON t.contact_id = as2.contact_id
LEFT JOIN ( 
				SELECT id, prefix_id, zip_code, town_clean, code_geo
				FROM contacts) as contacts
				ON contacts.id= as2.contact_id 
LEFT JOIN (
				SELECT c.contact_id, c.prefix_id,
				CASE WHEN c.prefix_id in ('MME','MLLE') THEN 1 ELSE 0 END AS is_female,
				CASE WHEN c.prefix_id in ('MMME') THEN 1 ELSE 0 END AS is_couple, 
				CASE WHEN c.prefix_id in ('MR') THEN 1 ELSE 0 END AS is_male,
				CASE WHEN c.prefix_id in ('AU','DR','NA','0') THEN 1 ELSE 0 END AS is_gender_unkown
				FROM 
				(
				SELECT * 
				FROM assignment2 as as2
				LEFT JOIN (
				SELECT id, IFNULL(prefix_id,0) as prefix_id
				FROM contacts) b
				ON b.id= as2.contact_id
				) c
) as gender
ON gender.contact_id= as2.contact_id
LEFT JOIN (

	SELECT as2.contact_id, 
	CASE WHEN b.donation >0 THEN 1 ELSE 0 END as donated_june17,
	CASE WHEN c.donation >0 THEN 1 ELSE 0 END as donated_june16
	FROM assignment2 as as2
			LEFT JOIN (
			SELECT contact_id, year(act_date), month(act_date), amount as donation
			FROM acts
			WHERE (month(act_date) in ('6') AND year(act_date) in ('2017'))
			GROUP BY 1,2,3) as b
			on b.contact_id = as2.contact_id
			LEFT JOIN( 
			SELECT contact_id, year(act_date), month(act_date), amount as donation
			FROM acts
			WHERE (month(act_date) in ('6') AND year(act_date) in ('2016'))
			GROUP BY 1,2,3) as c
			on c.contact_id = as2.contact_id
) AS binaries
ON binaries.contact_id= as2.contact_id
WHERE as2.calibration = 1
ORDER BY 2;
"
				
				calib_data = sqlQuery(db, query)
				#Close the connection
				odbcClose(db)
				
				rownames(calib_data) = calib_data$contact_id
				calib_data = calib_data[, -1]
				calib_data$num_solicitations <- as.numeric(calib_data$num_solicitations)
				calib_data$num_donations <- as.numeric(calib_data$num_donations)
				calib_data$pa_doner <- as.numeric(calib_data$pa_doner)
				
#----------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------	
#Probability_Model
				
				library(caTools)
				prob_split= sample.split(calib_data$will_donate, SplitRatio= 0.8)
				prob_train= subset(calib_data, prob_split == TRUE)
				prob_test = subset(calib_data, prob_split == FALSE) 
				
				library(smotefamily)
				balanced.data <- SMOTE(prob_train, prob_train$will_donate, dup_size=1)
				balanced_data = as.data.frame(balanced.data$data)
				
			
				library(glmnet)
				library(caret)
				#TRAINING
				rm(prob_x_train)
				rm(prob_y_train)
				prob_x_train <- model.matrix((will_donate ~ log(1+conversion_rate)+ conversion_rate  + log(1+years_since_latest_donation) + log(1+years_since_first_donation_ever)+ may_june_conversion +log(1+time_todonate_ratio)+ consistency+log(1+consistency)+ average_do_amount+ num_solicitations+ avg_summer_donations+ avg_overall_amount+ max_summer_donation+ num_donations+ num_summer_donations+ max_do_donation+ is_female+ is_male+ is_couple+ is_gender_unkown+ pa_doner+max_overall_donation+ donated_june16+ donated_june17), data= balanced_data)
				prob_y_train <- balanced_data$will_donate
				prob_x_test <- model.matrix((will_donate ~ log(1+conversion_rate)+ conversion_rate+ log(1+years_since_latest_donation) + log(1+years_since_first_donation_ever)+ may_june_conversion+log(1+time_todonate_ratio)+ consistency+ log(1+consistency)+ average_do_amount+ num_solicitations+ avg_summer_donations+ avg_overall_amount+ max_summer_donation+ num_donations+ num_summer_donations+ max_do_donation+ is_female+ is_male+ is_couple+ is_gender_unkown+ pa_doner+max_overall_donation+ donated_june16+ donated_june17), data= prob_test)
				prob_y_test <- prob_test$will_donate 
				
				#iterations 
				
				rm(prob_model)
				prob_model <- cv.glmnet(prob_x_train, prob_y_train, type.measure='mse', alpha=1, family="binomial")
				coef(prob_model)
				rm(prob_model_predicted)
				prob_model_predicted <- predict(prob_model, s=prob_model$lambda.1se,  newx=prob_x_test, type='response')
				
				mean((prob_y_test- prob_model_predicted)^2)
				
				
				#Extract_Probability
				#rm(prob_df)
				#prob_df = as.data.frame(t(prob_model_predicted))
				#library("writexl")
				#write_xlsx(prob_df,'C:/Users/sarah/Documents/DSBA/Marketing Analytics/Assignment2/new_trials//newprob_prediction12.xlsx')
				
#------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------

#Amount_Model
#Filtering_data
				z = which((calib_data$final_amount) != 0)
				calib_data_amount = calib_data[z, ]
				
				library(nnet)
				library(caTools)
				
				split= sample.split(calib_data_amount$final_amount, SplitRatio= 0.7)
				amount_train= subset(calib_data_amount, split == TRUE)
				amount_test= subset(calib_data_amount, split == FALSE) 
				
				library(glmnet)
				library(caret)
				
				amount_x_train <- model.matrix(log(final_amount)~ log(1+max_do_donation)+ log(1+average_do_amount)+ log(1+max_overall_donation)+log(1+avg_overall_amount)+log(1+max_summer_donation)+log(1+avg_summer_donations), data= amount_train)
				amount_y_train <- log(amount_train$final_amount)
				amount_x_test <- model.matrix(log(final_amount)~ log(1+max_do_donation)+ log(1+average_do_amount)+ log(1+max_overall_donation)+log(1+avg_overall_amount)+log(1+max_summer_donation)+log(1+avg_summer_donations), data= amount_test)
				amount_y_test <- log(amount_test$final_amount)
				amount_model <- cv.glmnet(amount_x_train, amount_y_train, type.measure="mse", alpha=0.6, family="gaussian")
				
				rm(amount_model_predicted)
				amount_model_predicted <- exp(predict(amount_model, s=amount_model$lambda.min,  newx= amount_x_test))
				mean((amount_y_test- amount_model_predicted)^2)
				coef(amount_model)
				
				#Extract_Amount
				#amount_df = as.data.frame(t(amount_model_predicted))
				#library("writexl")
				#write_xlsx(amount_df,'C:/Users/sarah/Documents/DSBA/Marketing Analytics/Assignment2//amount_prediction.xlsx')
				
				
#-------------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------
				
#SCORING
library(RODBC)
				
				#Open the connection
db = odbcConnect("my_data_source",uid="root",pwd="colombia")
sqlQuery(db, "USE ma_charity_full")
query=
"
SELECT 
as2.contact_id AS contact_id,
ifnull(as2.amount,0) AS final_amount,
ifnull(as2.donation,0) as will_donate,
ifnull(a.years_since_latest_donation,0) AS years_since_latest_donation, 
ifnull(a.years_since_first_donation_ever,0) AS years_since_first_donation_ever,  
ifnull(a.average_do_amount,0) AS average_do_amount, 
ifnull(a.num_donations,0) AS num_donations, 
ifnull(sol.num_solicitations,0) AS num_solicitations, 
ifnull(a.num_donations/sol.num_solicitations,0) AS conversion_rate, 
CASE WHEN pa.pa_donations > 0 THEN 1
	ELSE 0 END AS pa_doner, 
ifnull(a.years_since_latest_donation/((a.years_since_first_donation_ever-a.years_since_latest_donation)/a.num_donations),0) AS time_todonate_ratio,
ifnull((a.years_since_first_donation_ever-a.years_since_latest_donation)/a.num_donations,0) AS consistency,  #time_between_donations
ifnull(a.max_do_donation,0) AS max_do_donation, 
ifnull(c.max_overall_donation,0) AS max_overall_donation, 
ifnull(c.avg_overall_amount,0) AS avg_overall_amount, 
ifnull(d.max_summer_donation,0) AS max_summer_donation, 
ifnull(d.avg_summer_donations,0) AS avg_summer_donations,
ifnull(d.num_summer_donations,0) AS num_summer_donations, 
ifnull(t.may_june_donations/t.may_june_solicitations,0) as may_june_conversion,
gender.is_female as is_female,
gender.is_male as is_male, 
gender.is_gender_unkown as is_gender_unkown,
gender.is_couple as is_couple,
binaries.donated_june16 as donated_june16,
binaries.donated_june17 as donated_june17

FROM assignment2 as2
LEFT JOIN ( 
			SELECT contact_id, 
            DATEDIFF(20180625, MAX(act_date))/ 365 AS years_since_latest_donation, 
            DATEDIFF(20180625, MIN(act_date))/365 AS years_since_first_donation_ever,
			avg(amount) AS average_do_amount,
            COUNT(amount) AS num_donations,
            IFNULL(MAX(amount),0) AS max_do_donation
			FROM acts
            WHERE act_type_id= 'DO' 
			GROUP BY 1) AS a
            on a.contact_id= as2.contact_id
LEFT JOIN ( 
SELECT contact_id,
COUNT(action_date) AS num_solicitations
FROM actions
GROUP BY 1) AS sol
on sol.contact_id = as2.contact_id
LEFT JOIN (
			SELECT contact_id, 
            IFNULL(MAX(amount),0) as max_overall_donation,
            IFNULL(avg(amount),0) as avg_overall_amount
            FROM acts 
            GROUP BY 1) as c
            on c.contact_id = as2.contact_id
LEFT JOIN(
			SELECT contact_id, 
            IFNULL(MAX(amount),0) as max_summer_donation,
            IFNULL(count(act_date),0) as num_summer_donations, 
            IFNULL(AVG(amount),0) as avg_summer_donations
            FROM acts
            WHERE month(act_date) in (5,6,7,8)
            GROUP BY 1) as d
            ON d.contact_id = as2.contact_id
LEFT JOIN(
			SELECT contact_id, SUM(amount) as pa_donations
            FROM acts 
            WHERE act_type_id = 'PA'
            GROUP BY 1) as pa
            on pa.contact_id = as2.contact_id
LEFT JOIN 
(
				SELECT a.contact_id, count(action_date) as may_june_solicitations, ifnull(b.may_june_donations,0) as may_june_donations #the contact, the campaign and the number of solicitation per campaign in May June
				FROM actions a
				LEFT JOIN (
				SELECT contact_id, count(act_date) as may_june_donations
				FROM acts
				WHERE (MONTH(act_date) in (5,6) AND YEAR(act_date) in (2015,2016,2017,2018))
				GROUP BY 1) as b
				on b.contact_id= a.contact_id
				WHERE (MONTH(a.action_date) in (5,6) AND YEAR(action_date) in (2015,2016,2017,2018))
				GROUP BY 1
) t ON t.contact_id = as2.contact_id
LEFT JOIN ( 
				SELECT id, prefix_id, zip_code, town_clean, code_geo
				FROM contacts) as contacts
				ON contacts.id= as2.contact_id 
LEFT JOIN (
				SELECT c.contact_id, c.prefix_id,
				CASE WHEN c.prefix_id in ('MME','MLLE') THEN 1 ELSE 0 END AS is_female,
				CASE WHEN c.prefix_id in ('MMME') THEN 1 ELSE 0 END AS is_couple, 
				CASE WHEN c.prefix_id in ('MR') THEN 1 ELSE 0 END AS is_male,
				CASE WHEN c.prefix_id in ('AU','DR','NA','0') THEN 1 ELSE 0 END AS is_gender_unkown
				FROM 
				(
				SELECT * 
				FROM assignment2 as as2
				LEFT JOIN (
				SELECT id, IFNULL(prefix_id,0) as prefix_id
				FROM contacts) b
				ON b.id= as2.contact_id
				) c
) as gender
ON gender.contact_id= as2.contact_id
LEFT JOIN (

	SELECT as2.contact_id, 
	CASE WHEN b.donation >0 THEN 1 ELSE 0 END as donated_june17,
	CASE WHEN c.donation >0 THEN 1 ELSE 0 END as donated_june16
	FROM assignment2 as as2
			LEFT JOIN (
			SELECT contact_id, year(act_date), month(act_date), amount as donation
			FROM acts
			WHERE (month(act_date) in ('6') AND year(act_date) in ('2017'))
			GROUP BY 1,2,3) as b
			on b.contact_id = as2.contact_id
			LEFT JOIN( 
			SELECT contact_id, year(act_date), month(act_date), amount as donation
			FROM acts
			WHERE (month(act_date) in ('6') AND year(act_date) in ('2016'))
			GROUP BY 1,2,3) as c
			on c.contact_id = as2.contact_id
) AS binaries
ON binaries.contact_id= as2.contact_id
WHERE as2.calibration = 0
ORDER BY 2;
"
newdata = sqlQuery(db, query)
#Close the connection
odbcClose(db)
#rownames(newdata) = newdata$contact_id
#newdata = newdata[, -1]
				
				x_predict_amount <- model.matrix(log(final_amount)~ log(1+max_do_donation)+ log(1+average_do_amount)+ log(1+max_overall_donation)+log(1+avg_overall_amount)+log(1+max_summer_donation)+log(1+avg_summer_donations), data= newdata)
				amount <- exp(predict(amount_model, s=amount_model$lambda.min,  newx=x_predict_amount))
				
				x_predict_prob <- model.matrix((will_donate ~ log(1+conversion_rate)+ conversion_rate  + log(1+years_since_latest_donation) + log(1+years_since_first_donation_ever)+ may_june_conversion +log(1+time_todonate_ratio)+ consistency+log(1+consistency)+ average_do_amount+ num_solicitations+ avg_summer_donations+ avg_overall_amount+ max_summer_donation+ num_donations+ num_summer_donations+ max_do_donation+ is_female+ is_male+ is_couple+ is_gender_unkown+ pa_doner+max_overall_donation+ donated_june16+ donated_june17), data= newdata)
				prob <- predict(prob_model, s=prob_model$lambda.1se,  newx=x_predict_prob, type='response')
				
				out = data.frame(contact_id = newdata$contact_id)
				out$prob  = prob
				out$amount = amount
				out$score  = out$prob * out$amount
				out$solicit = ifelse(out$score<2,0,1)
				z = which(out$solicit ==1)
				print(length(z))
				submission_df= subset(out, select = c(contact_id, solicit))
				submission_df= submission_df[order(submission_df$contact_id),]
				
				#library(data.table)
				#write.table(submission_df, file='submissionnew.txt', sep="\t", row.names= FALSE, col.names= FALSE)
				#library("writexl")
				#write_xlsx(submission_df,'C:/Users/sarah/Documents/DSBA/Marketing Analytics/Assignment2//submission_dfnew2.xlsx')
				