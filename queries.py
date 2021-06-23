import datetime
import time

import pandas as pd


def add_to_date(date, **kwargs):
    new_date = datetime.datetime.fromisoformat(date) + datetime.timedelta(**kwargs)
    new_date = new_date.isoformat().split('T')[0]
    return new_date


class Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class BaseQuery:
    template = """"""

    def render(self):
        return self.template

    def run(self, cur):
        query_string = self.render()

        start = time.time()
        result = cur.execute(query_string)
        end = time.time()
        completion_time = end - start
        return result, completion_time


class Query1(BaseQuery):
    template = """SELECT L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY) AS SUM_QTY,
     SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS SUM_DISC_PRICE,
     SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)) AS SUM_CHARGE, AVG(L_QUANTITY) AS AVG_QTY,
     AVG(L_EXTENDEDPRICE) AS AVG_PRICE, AVG(L_DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER
    FROM LINEITEM
    WHERE L_SHIPDATE <= DATE('{date}') 
    GROUP BY L_RETURNFLAG, L_LINESTATUS
    ORDER BY L_RETURNFLAG,L_LINESTATUS;"""

    def __init__(self, days=-90):
        self.days = days

    def render(self):
        date = add_to_date('1998-12-01', days=self.days)
        query = self.template.format(date=date)
        return query


class Query2(BaseQuery):
    template = """
    SELECT S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT
    FROM PART, SUPPLIER, PARTSUPP, NATION, REGION
    WHERE P_PARTKEY = PS_PARTKEY AND S_SUPPKEY = PS_SUPPKEY AND P_SIZE = 15 AND
    P_TYPE LIKE '%BRASS' AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY AND
    R_NAME = 'EUROPE' AND
    PS_SUPPLYCOST = (SELECT MIN(PS_SUPPLYCOST) FROM PARTSUPP, SUPPLIER, NATION, REGION
     WHERE P_PARTKEY = PS_PARTKEY AND S_SUPPKEY = PS_SUPPKEY
     AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY AND R_NAME = 'EUROPE')
    ORDER BY S_ACCTBAL, N_NAME, S_NAME, P_PARTKEY
    LIMIT 100;
    """


class Query3(BaseQuery):
    template = """
    SELECT L_ORDERKEY, SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS REVENUE, O_ORDERDATE, O_SHIPPRIORITY
    FROM CUSTOMER, ORDERS, LINEITEM
    WHERE C_MKTSEGMENT = 'BUILDING' AND C_CUSTKEY = O_CUSTKEY AND L_ORDERKEY = O_ORDERKEY AND
    O_ORDERDATE < '1995-03-15' AND L_SHIPDATE > '1995-03-15'
    GROUP BY L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
    ORDER BY REVENUE DESC, O_ORDERDATE
    LIMIT 10;"""


class Query4(BaseQuery):
    template = """
    SELECT O_ORDERPRIORITY, COUNT(*) AS ORDER_COUNT FROM ORDERS
    WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '{date}'
    AND EXISTS (SELECT * FROM LINEITEM WHERE L_ORDERKEY = O_ORDERKEY AND L_COMMITDATE < L_RECEIPTDATE)
    GROUP BY O_ORDERPRIORITY
    ORDER BY O_ORDERPRIORITY;"""

    def render(self):
        # date = add_to_date('1993-07-01', months=3)
        return self.template.format(date='1993-10-01')


class Query5(BaseQuery):
    template = """
    SELECT N_NAME, SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS REVENUE
    FROM CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION
    WHERE C_CUSTKEY = O_CUSTKEY AND L_ORDERKEY = O_ORDERKEY AND L_SUPPKEY = S_SUPPKEY
    AND C_NATIONKEY = S_NATIONKEY AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY
    AND R_NAME = 'ASIA' AND O_ORDERDATE >= '1994-01-01'
    AND O_ORDERDATE < '{date}'
    GROUP BY N_NAME
    ORDER BY REVENUE DESC;"""

    def render(self):
        # add 1 year to '1994-01-01'
        date = '1995-01-01'
        return self.template.format(date=date)


class Query6(BaseQuery):
    template = """
    SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
    FROM LINEITEM
    WHERE L_SHIPDATE >= DATE('1994-01-01') AND L_SHIPDATE < DATE('1994-01-01', '+1 year')
    AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01 AND L_QUANTITY < 24;"""


class Query7(BaseQuery):
    template = """
    SELECT SUPP_NATION, CUST_NATION, L_YEAR, SUM(VOLUME) AS REVENUE
    FROM ( SELECT N1.N_NAME AS SUPP_NATION, N2.N_NAME AS CUST_NATION, strftime('%Y', L_SHIPDATE) AS L_YEAR,
     L_EXTENDEDPRICE*(1-L_DISCOUNT) AS VOLUME
     FROM SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION N1, NATION N2
     WHERE S_SUPPKEY = L_SUPPKEY AND O_ORDERKEY = L_ORDERKEY AND C_CUSTKEY = O_CUSTKEY
     AND S_NATIONKEY = N1.N_NATIONKEY AND C_NATIONKEY = N2.N_NATIONKEY AND
     ((N1.N_NAME = 'FRANCE' AND N2.N_NAME = 'GERMANY') OR
     (N1.N_NAME = 'GERMANY' AND N2.N_NAME = 'FRANCE')) AND
     L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31' ) AS SHIPPING
    GROUP BY SUPP_NATION, CUST_NATION, L_YEAR
    ORDER BY SUPP_NATION, CUST_NATION, L_YEAR;"""


class Query8(BaseQuery):
    template = """
    SELECT O_YEAR, SUM(CASE WHEN NATION = 'BRAZIL' THEN VOLUME ELSE 0 END)/SUM(VOLUME) AS MKT_SHARE
    FROM (SELECT strftime('%Y', O_ORDERDATE) AS O_YEAR, L_EXTENDEDPRICE*(1-L_DISCOUNT) AS VOLUME, N2.N_NAME AS NATION
     FROM PART, SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION N1, NATION N2, REGION
     WHERE P_PARTKEY = L_PARTKEY AND S_SUPPKEY = L_SUPPKEY AND L_ORDERKEY = O_ORDERKEY
     AND O_CUSTKEY = C_CUSTKEY AND C_NATIONKEY = N1.N_NATIONKEY AND
     N1.N_REGIONKEY = R_REGIONKEY AND R_NAME = 'AMERICA' AND S_NATIONKEY = N2.N_NATIONKEY
     AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31' AND P_TYPE= 'ECONOMY ANODIZED STEEL') AS ALL_NATIONS
    GROUP BY O_YEAR
    ORDER BY O_YEAR;"""


class Query9(BaseQuery):
    template = """SELECT NATION, O_YEAR, SUM(AMOUNT) AS SUM_PROFIT
    FROM (SELECT N_NAME AS NATION, strftime('%Y', O_ORDERDATE) AS O_YEAR,
     L_EXTENDEDPRICE*(1-L_DISCOUNT)-PS_SUPPLYCOST*L_QUANTITY AS AMOUNT
     FROM PART, SUPPLIER, LINEITEM, PARTSUPP, ORDERS, NATION
     WHERE S_SUPPKEY = L_SUPPKEY AND PS_SUPPKEY= L_SUPPKEY AND PS_PARTKEY = L_PARTKEY AND
     P_PARTKEY= L_PARTKEY AND O_ORDERKEY = L_ORDERKEY AND S_NATIONKEY = N_NATIONKEY AND
     P_NAME LIKE '%%green%%') AS PROFIT
    GROUP BY NATION, O_YEAR
    ORDER BY NATION, O_YEAR DESC;"""


class Query10(BaseQuery):
    template = """
SELECT C_CUSTKEY, C_NAME, SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS REVENUE, C_ACCTBAL,
N_NAME, C_ADDRESS, C_PHONE, C_COMMENT
FROM CUSTOMER, ORDERS, LINEITEM, NATION
WHERE C_CUSTKEY = O_CUSTKEY AND L_ORDERKEY = O_ORDERKEY AND O_ORDERDATE>= '1993-10-01' AND
O_ORDERDATE < '{date}' AND
L_RETURNFLAG = 'R' AND C_NATIONKEY = N_NATIONKEY
GROUP BY C_CUSTKEY, C_NAME, C_ACCTBAL, C_PHONE, N_NAME, C_ADDRESS, C_COMMENT
ORDER BY REVENUE DESC LIMIT 20;"""

    def render(self):
        # date = add_to_date('1993-10-01', months +=3)
        return self.template.format(date='1994-01-01')


class Query11(BaseQuery):
    template = """SELECT PS_PARTKEY, SUM(PS_SUPPLYCOST*PS_AVAILQTY) AS VALUE
    FROM PARTSUPP, SUPPLIER, NATION
    WHERE PS_SUPPKEY = S_SUPPKEY AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'GERMANY'
    GROUP BY PS_PARTKEY
    HAVING SUM(PS_SUPPLYCOST*PS_AVAILQTY) > (SELECT SUM(PS_SUPPLYCOST*PS_AVAILQTY) * 0.0001000000
     FROM PARTSUPP, SUPPLIER, NATION
     WHERE PS_SUPPKEY = S_SUPPKEY AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'GERMANY')
    ORDER BY VALUE DESC;"""

    def render(self):
        return self.template


class Query12(BaseQuery):
    template = """SELECT L_SHIPMODE,
    SUM(CASE WHEN O_ORDERPRIORITY = '1-URGENT' OR O_ORDERPRIORITY = '2-HIGH' THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT,
    SUM(CASE WHEN O_ORDERPRIORITY <> '1-URGENT' AND O_ORDERPRIORITY <> '2-HIGH' THEN 1 ELSE 0 END ) AS LOW_LINE_COUNT
    FROM ORDERS, LINEITEM
    WHERE O_ORDERKEY = L_ORDERKEY AND L_SHIPMODE IN ('MAIL','SHIP')
    AND L_COMMITDATE < L_RECEIPTDATE AND L_SHIPDATE < L_COMMITDATE AND L_RECEIPTDATE >= DATE('1994-01-01')
    AND L_RECEIPTDATE < DATE('1994-01-01', '+1 year')
    GROUP BY L_SHIPMODE
    ORDER BY L_SHIPMODE;"""



class Query13(BaseQuery):
    template = """
    SELECT
	C_COUNT,
	COUNT(*) as CUSTDIST
from
	(
		SELECT
			C_CUSTKEY,
			COUNT(O_ORDERKEY) as C_COUNT
		from
			CUSTOMER LEFT OUTER JOIN ORDERS ON
				C_CUSTKEY = O_CUSTKEY
				AND O_COMMENT NOT LIKE '%special%requests%'
		GROUP BY
			C_CUSTKEY
	) AS C_ORDERS
GROUP BY
	C_COUNT
ORDER BY
	CUSTDIST DESC,
	C_COUNT DESC;"""


class Query14(BaseQuery):
    template = """SELECT 100.00* SUM(CASE WHEN P_TYPE LIKE 'PROMO%%' THEN L_EXTENDEDPRICE*(1-L_DISCOUNT)
ELSE 0 END) / SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS PROMO_REVENUE
FROM LINEITEM, PART
WHERE L_PARTKEY = P_PARTKEY AND L_SHIPDATE >= '1995-09-01' AND L_SHIPDATE < '{date}';"""

    def render(self):
        #date'1995-09-01' + 1 month
        return self.template.format(date= '1995-10-01')


class Query15(BaseQuery):
    view_template = """
    create view revenue0 (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= date('1996-01-01')
		and l_shipdate < date('1996-01-01', '+3 month')
	group by
		l_suppkey;"""

    template = """
    select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	revenue0
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue0
	)
order by
	s_suppkey;"""

    def __init__(self, view_name='REVENUE0'):
        self.view_name = view_name

    def render_view(self):
        # add 3 months to '1996-01-01'
        date = '1996-04-01'
        return self.view_template #.format(name=self.view_name, date=date)

    def render(self):
        return self.template

    def run(self, cur):
        query_string = self.render()
        view_query = self.render_view()

        start = time.time()
        try:
            view_result = cur.execute(view_query)
        except:
            cur.execute(f'DROP VIEW {self.view_name}')
            raise Exception('View already existed. Dropped View')

        result = cur.execute(query_string)
        results = result.fetchall()
        result = Result(results)
        end = time.time()
        cur.execute(f'DROP VIEW {self.view_name}')
        completion_time = end - start
        return result, completion_time



class Query16(BaseQuery):
    template = """
    SELECT P_BRAND, P_TYPE, P_SIZE, COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT
    FROM PARTSUPP, PART
    WHERE P_PARTKEY = PS_PARTKEY AND P_BRAND <> 'Brand#45' AND P_TYPE NOT LIKE 'MEDIUM POLISHED%%'
    AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9) AND PS_SUPPKEY NOT IN (SELECT S_SUPPKEY FROM SUPPLIER
     WHERE S_COMMENT LIKE '%%Customer%%Complaints%%')
    GROUP BY P_BRAND, P_TYPE, P_SIZE
    ORDER BY SUPPLIER_CNT DESC, P_BRAND, P_TYPE, P_SIZE;"""


class Query17(BaseQuery):
    template = """
    SELECT SUM(L_EXTENDEDPRICE)/7.0 AS AVG_YEARLY FROM LINEITEM, PART
    WHERE P_PARTKEY = L_PARTKEY AND P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BOX'
    AND L_QUANTITY < (SELECT 0.2*AVG(L_QUANTITY) FROM LINEITEM WHERE L_PARTKEY = P_PARTKEY);"""


class OptimizedQuery17(BaseQuery):
    template = """
    SELECT SUM(L_EXTENDEDPRICE)/7.0 AS AVG_YEARLY FROM LINEITEM, PART
    WHERE P_PARTKEY = L_PARTKEY AND P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BOX'
    AND L_QUANTITY < (SELECT 0.2*AVG(L_QUANTITY) FROM LINEITEM, PART WHERE L_PARTKEY = P_PARTKEY);"""

    def run(self, cur):
        first_query = """SELECT L_QUANTITY, L_EXTENDEDPRICE, P_BRAND, P_CONTAINER, L_PARTKEY FROM LINEITEM, PART WHERE L_PARTKEY = P_PARTKEY AND P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BOX';"""
        start = time.time()
        result = cur.execute(first_query)
        records = [i for i in result.fetchall()]
        df = pd.DataFrame(records, columns=['L_QUANTITY', 'L_EXTENDEDPRICE', 'P_BRAND', 'P_CONTAINER', 'L_PARTKEY'])
        df.to_csv('query17.csv', index=False)

        partkey_quantities = {}
        for i in df.iterrows():
            row = i[1]
            if row['L_PARTKEY'] not in partkey_quantities:
                partkey_quantities[row['L_PARTKEY']] = [row['L_QUANTITY']]
                continue
            partkey_quantities[row['L_PARTKEY']].append(row['L_QUANTITY'])

        averages = {}
        for k, v in partkey_quantities.items():
            averages[k] = sum(v) / len(v)

        data = []
        for i in df.iterrows():
            row = i[1]
            if row['L_QUANTITY'] < .2 * averages[row['L_PARTKEY']]:
                data.append(i)

        results = [(sum([i[1]['L_EXTENDEDPRICE'] for i in data]) / 7,)]
        result = Result(results)

        end = time.time()
        completion_time = end - start
        return result, completion_time



class Query18(BaseQuery):
    template = """
    SELECT C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, SUM(L_QUANTITY)
    FROM CUSTOMER, ORDERS, LINEITEM
    WHERE O_ORDERKEY IN (SELECT L_ORDERKEY FROM LINEITEM GROUP BY L_ORDERKEY HAVING
     SUM(L_QUANTITY) > 300) AND C_CUSTKEY = O_CUSTKEY AND O_ORDERKEY = L_ORDERKEY
    GROUP BY C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE
    ORDER BY O_TOTALPRICE DESC, O_ORDERDATE
    LIMIT 100;"""


class Query19(BaseQuery):
    template = """
    SELECT SUM(L_EXTENDEDPRICE* (1 - L_DISCOUNT)) AS REVENUE
    FROM LINEITEM, PART
    WHERE (P_PARTKEY = L_PARTKEY AND P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND L_QUANTITY >= 1 AND L_QUANTITY <= 1 + 10 AND P_SIZE BETWEEN 1 AND 5
    AND L_SHIPMODE IN ('AIR', 'AIR REG') AND L_SHIPINSTRUCT = 'DELIVER IN PERSON')
    OR (P_PARTKEY = L_PARTKEY AND P_BRAND ='Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND L_QUANTITY >=10 AND L_QUANTITY <=10 + 10 AND P_SIZE BETWEEN 1 AND 10
    AND L_SHIPMODE IN ('AIR', 'AIR REG') AND L_SHIPINSTRUCT = 'DELIVER IN PERSON')
    OR (P_PARTKEY = L_PARTKEY AND P_BRAND = 'Brand#34' AND P_CONTAINER IN ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND L_QUANTITY >=20 AND L_QUANTITY <= 20 + 10 AND P_SIZE BETWEEN 1 AND 15
    AND L_SHIPMODE IN ('AIR', 'AIR REG') AND L_SHIPINSTRUCT = 'DELIVER IN PERSON');"""


class Query20(BaseQuery):
    template = """
    SELECT S_NAME, S_ADDRESS FROM SUPPLIER, NATION
    WHERE S_SUPPKEY IN (SELECT PS_SUPPKEY FROM PARTSUPP
     WHERE PS_PARTKEY IN (SELECT P_PARTKEY FROM PART WHERE P_NAME like 'forest%%') AND
     PS_AVAILQTY > (SELECT 0.5*sum(L_QUANTITY) FROM LINEITEM WHERE L_PARTKEY = PS_PARTKEY AND
      L_SUPPKEY = PS_SUPPKEY AND L_SHIPDATE >= '1994-01-01' AND
      L_SHIPDATE < '{date}')) AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'CANADA'
    ORDER BY S_NAME;"""

    def render(self):
        # add 1 year to '1994-01-01'
        date = '1995-01-01'
        return self.template.format(date=date)


class OptimizedQuery20(BaseQuery):
    template = """
    SELECT S_NAME, S_ADDRESS FROM SUPPLIER, NATION
    WHERE S_SUPPKEY IN (SELECT PS_SUPPKEY FROM PARTSUPP
     WHERE PS_PARTKEY IN (SELECT P_PARTKEY FROM PART WHERE P_NAME like 'forest%%') AND
     PS_AVAILQTY > (SELECT 0.5*sum(L_QUANTITY) FROM LINEITEM WHERE L_PARTKEY = PS_PARTKEY AND
      L_SUPPKEY = PS_SUPPKEY AND L_SHIPDATE >= '1994-01-01' AND
      L_SHIPDATE < '{date}')) AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'CANADA'
    ORDER BY S_NAME;"""

    def render(self):
        # add 1 year to '1994-01-01'
        date = '1995-01-01'
        return self.template.format(date=date)

"""
    SELECT S_NAME, S_ADDRESS, S_SUPPKEY FROM SUPPLIER, NATION
    WHERE S_SUPPKEY IN (SELECT PS_SUPPKEY FROM PARTSUPP
     WHERE PS_PARTKEY IN (SELECT P_PARTKEY FROM PART WHERE P_NAME like 'forest%%') AND
     PS_AVAILQTY > (SELECT 0.5*sum(L_QUANTITY) FROM LINEITEM WHERE L_PARTKEY = PS_PARTKEY AND
      L_SUPPKEY = PS_SUPPKEY AND L_SHIPDATE >= '1994-01-01' AND
      L_SHIPDATE < '1995-01-01') LIMIT 5000) AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'CANADA'
    ORDER BY S_NAME;"""

class Query21(BaseQuery):
    template = """
    SELECT S_NAME, COUNT(*) AS NUMWAIT
    FROM SUPPLIER, LINEITEM L1, ORDERS, NATION WHERE S_SUPPKEY = L1.L_SUPPKEY AND
    O_ORDERKEY = L1.L_ORDERKEY AND O_ORDERSTATUS = 'F' AND L1.L_RECEIPTDATE> L1.L_COMMITDATE
    AND EXISTS (SELECT * FROM LINEITEM L2 WHERE L2.L_ORDERKEY = L1.L_ORDERKEY
     AND L2.L_SUPPKEY <> L1.L_SUPPKEY) AND
    NOT EXISTS (SELECT * FROM LINEITEM L3 WHERE L3.L_ORDERKEY = L1.L_ORDERKEY AND
     L3.L_SUPPKEY <> L1.L_SUPPKEY AND L3.L_RECEIPTDATE > L3.L_COMMITDATE) AND
    S_NATIONKEY = N_NATIONKEY AND N_NAME = 'SAUDI ARABIA'
    GROUP BY S_NAME
    ORDER BY NUMWAIT DESC, S_NAME
    LIMIT 100;"""


class Query22(BaseQuery):
    template = """
    SELECT CNTRYCODE, COUNT(*) AS NUMCUST, SUM(C_ACCTBAL) AS TOTACCTBAL
    FROM (SELECT SUBSTRING(C_PHONE,1,2) AS CNTRYCODE, C_ACCTBAL
     FROM CUSTOMER WHERE SUBSTRING(C_PHONE,1,2) IN ('13', '31', '23', '29', '30', '18', '17') AND
     C_ACCTBAL > (SELECT AVG(C_ACCTBAL) FROM CUSTOMER WHERE C_ACCTBAL > 0.00 AND
      SUBSTRING(C_PHONE,1,2) IN ('13', '31', '23', '29', '30', '18', '17')) AND
     NOT EXISTS ( SELECT * FROM ORDERS WHERE O_CUSTKEY = C_CUSTKEY)) AS CUSTSALE
    GROUP BY CNTRYCODE
    ORDER BY CNTRYCODE;"""



queries = {
    'query_1': Query1(days=-90),
    'query_2': Query2(),
    'query_3': Query3(),
    'query_4': Query4(),
    'query_5': Query5(),
    'query_6': Query6(),
    'query_7': Query7(),
    'query_8': Query8(),
    'query_9': Query9(),
    'query_10': Query10(),
    'query_11': Query11(),
    'query_12': Query12(),
    'query_13': Query13(),
    'query_14': Query14(),
    'query_15': Query15(),
    'query_16': Query16(),
    'query_17': Query17(),
    'query_18': Query18(),
    'query_19': Query19(),
    'query_20': Query20(),
    'query_21': Query21(),
    'query_22': Query22(),
    'optimized_query_17': OptimizedQuery17()
}

