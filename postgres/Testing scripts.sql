------------ Testing Doc ---------------------
----------------------------------------------

---------------------------------------------------------------------------------------------------
-----------------------------------------------Schema: Public -------------------------------------
---------------------------------------------------------------------------------------------------
-------------Customers------------------------

select  substring(created_at::text,1,10) 
	   ,count(*) customers
from public.customers 
group by 1
order by 1 desc;

-------------Accounts-------------------------

select  substring(created_at,1,10) 
       ,count(*) accounts
       --,sum(balance) bal
from banking.public.accounts da
group by 1
order by 1 desc;


-------------Transactions---------------------

select substring(transaction_time::text,1,10)
	  ,count(*) transactions
	  ,sum(amount) amount
from cleaned.fact_transactions ft 
group by 1
order by 1 desc;

---------------------------------------------------------------------------------------------------
-----------------------------------------------Schema: Raw ----------------------------------------
---------------------------------------------------------------------------------------------------
-------------Customers------------------------

select  substring(created_at::text,1,10) 
	   ,count(*) customers
from raw.customers dc
group by 1
order by 1 desc;


select  DATE_TRUNC('day', created_at)::date AS created_date
	   ,count(*) customers
from cleaned.stg_customers
group by 1
order by 1 desc;


select  DATE_TRUNC('day', created_at)::date AS created_date
	   ,count(*) customers
from cleaned.customers_snapshot
group by 1
order by 1 desc;


select  DATE_TRUNC('day', created_at)::date AS created_date
	   ,count(*) customers
from cleaned.dim_customers
group by 1
order by 1 desc;


SELECT DATE_TRUNC('day', created_at)::date AS created_date, count(*)
FROM business_ready.dim_customers
GROUP BY 1
ORDER BY 1 DESC;


-------------Accounts-------------------------

select  substring(created_at::text,1,10) 
       ,count(*) accounts
       ,sum(balance) bal
from cleaned.dim_accounts da
group by 1
order by 1 desc;


-------------Transactions---------------------

select substring(transaction_time::text,1,10)
	  ,count(*) transactions
	  ,sum(amount) amount
from cleaned.fact_transactions ft 
group by 1
order by 1 desc;



---------------------------------------------------------------------------------------------------
-----------------------------------------------Schema: Cleaned ------------------------------------
---------------------------------------------------------------------------------------------------
-------------Customers------------------------

select  substring(created_at::text,1,10) 
	   ,count(*) customers
from cleaned.dim_customers dc
group by 1
order by 1 desc;

-------------Accounts-------------------------

select  substring(created_at::text,1,10) 
       ,count(*) accounts
       ,sum(balance) bal
from cleaned.dim_accounts da
group by 1
order by 1 desc;


-------------Transactions---------------------

select substring(transaction_time::text,1,10)
	  ,count(*) transactions
	  ,sum(amount) amount
from cleaned.fact_transactions ft 
group by 1
order by 1 desc;

SHOW wal_level; 
SELECT * FROM pg_replication_slots;




SELECT id, count(*) 
FROM raw.customers 
GROUP BY id 
HAVING count(*) > 1
ORDER BY count(*) DESC;
