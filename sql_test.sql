Use DB1;

-- How many total messages are being sent every day? Answer in the result of the query
select DATE(createdAt) as  message_date, count(id) as daily_message_count from message 
group by DATE(createdAt);

-- Are there any users that did not receive any message? Answer: Yes
select u.* from users u 
left join message m on u.userId = m.receiverId
where m.receiverId is null;

-- How many active subscriptions do we have today? Answer: 3
select count(*) as active_subscriptions_today from subscription 
where endDate >= current_date() and status = 'Active';

-- Are there users sending messages without an active subscription? Answer: Yes
select Distinct UserId from users u 
inner join subscription s on u.Hash_column = s.Hash_column
inner join message m on u.userId = m.senderId
where s.status != 'Active';

/*
Did you identified any inaccurate/noisy record that somehow could prejudice the data analyses? 
How to monitor it (SQL query)? Please explain how do you suggest to handle with this noisy data?

Answer: Yes we have some data received with null values which doesn't hold the required information, We can
preprocess the data to validate wheather we have all the required information or not. 
We can also use the below sample SQL query to check and find the noisy data where we dont have city details
*/
Select userId, case when city= 'None' then 'Noisy Data' Else 'Good Data' END as Data_Quality from users;

