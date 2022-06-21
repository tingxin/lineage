# coding=utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sqlparse
from sqlparse.sql import Identifier, IdentifierList
from sqlparse.tokens import Keyword, Name

RESULT_OPERATIONS = {'UNION', 'INTERSECT', 'EXCEPT', 'SELECT'}
ON_KEYWORD = 'ON'
PRECEDES_TABLE_NAME = {'FROM', 'JOIN', 'DESC', 'DESCRIBE', 'WITH'}


class BaseExtractor(object):
    def __init__(self, sql_statement):
        self.sql = sqlparse.format(
            sql_statement, reindent=True, keyword_case='upper')
        self._table_names = set()
        self._alias_names = set()
        self._limit = None
        self._parsed = sqlparse.parse(self.stripped())
        for statement in self._parsed:
            self.__extract_from_token(statement)
            self._limit = self._extract_limit_from_query(statement)
        self._table_names = self._table_names - self._alias_names

    @property
    def tables(self):
        return self._table_names

    @property
    def limit(self):
        return self._limit

    def is_select(self):
        return self._parsed[0].get_type() == 'SELECT'

    def is_explain(self):
        return self.stripped().upper().startswith('EXPLAIN')

    def is_readonly(self):
        return self.is_select() or self.is_explain()

    def stripped(self):
        return self.sql.strip(' \t\n;')

    def get_statements(self):
        statements = []
        for statement in self._parsed:
            if statement:
                sql = str(statement).strip(' \n;\t')
                if sql:
                    statements.append(sql)
        return statements

    @staticmethod
    def __precedes_table_name(token_value):
        for keyword in PRECEDES_TABLE_NAME:
            if keyword in token_value:
                return True
        return False

    @staticmethod
    def get_full_name(identifier):
        if len(identifier.tokens) > 1 and identifier.tokens[1].value == '.':
            return '{}.{}'.format(identifier.tokens[0].value,
                                  identifier.tokens[2].value)
        return identifier.get_real_name()

    @staticmethod
    def __is_result_operation(keyword):
        for operation in RESULT_OPERATIONS:
            if operation in keyword.upper():
                return True
        return False

    @staticmethod
    def __is_identifier(token):
        return isinstance(token, (IdentifierList, Identifier))

    def __process_identifier(self, identifier):
        if '(' not in '{}'.format(identifier):
            self._table_names.add(self.get_full_name(identifier))
            return

        # store aliases
        if hasattr(identifier, 'get_alias'):
            self._alias_names.add(identifier.get_alias())
        if hasattr(identifier, 'tokens'):
            # some aliases are not parsed properly
            if identifier.tokens[0].ttype == Name:
                self._alias_names.add(identifier.tokens[0].value)
        self.__extract_from_token(identifier)

    def as_create_table(self, table_name, overwrite=False):
        exec_sql = ''
        sql = self.stripped()
        if overwrite:
            exec_sql = 'DROP TABLE IF EXISTS {};\n'.format(table_name)
        exec_sql += 'CREATE TABLE {} AS \n{}'.format(table_name, sql)
        return exec_sql

    def __extract_from_token(self, token):
        if not hasattr(token, 'tokens'):
            return

        table_name_preceding_token = False

        for item in token.tokens:
            if item.is_group and not self.__is_identifier(item):
                self.__extract_from_token(item)

            if item.ttype in Keyword:
                if self.__precedes_table_name(item.value.upper()):
                    table_name_preceding_token = True
                    continue

            if not table_name_preceding_token:
                continue

            if item.ttype in Keyword or item.value == ',':
                if (self.__is_result_operation(item.value) or
                        item.value.upper() == ON_KEYWORD):
                    table_name_preceding_token = False
                    continue
                # FROM clause is over
                break

            if isinstance(item, Identifier):
                self.__process_identifier(item)

            if isinstance(item, IdentifierList):
                for token in item.tokens:
                    if self.__is_identifier(token):
                        self.__process_identifier(token)

    def _get_limit_from_token(self, token):
        if token.ttype == sqlparse.tokens.Literal.Number.Integer:
            return int(token.value)
        elif token.is_group:
            return int(token.get_token_at_offset(1).value)

    def _extract_limit_from_query(self, statement):
        limit_token = None
        for pos, item in enumerate(statement.tokens):
            if item.ttype in Keyword and item.value.lower() == 'limit':
                limit_token = statement.tokens[pos + 2]
                return self._get_limit_from_token(limit_token)

    def get_query_with_new_limit(self, new_limit):
        if not self._limit:
            return self.sql + ' LIMIT ' + str(new_limit)
        limit_pos = None
        tokens = self._parsed[0].tokens
        # Add all items to before_str until there is a limit
        for pos, item in enumerate(tokens):
            if item.ttype in Keyword and item.value.lower() == 'limit':
                limit_pos = pos
                break
        limit = tokens[limit_pos + 2]
        if limit.ttype == sqlparse.tokens.Literal.Number.Integer:
            tokens[limit_pos + 2].value = new_limit
        elif limit.is_group:
            tokens[limit_pos + 2].value = (
                '{}, {}'.format(next(limit.get_identifiers()), new_limit)
            )

        str_res = ''
        for i in tokens:
            str_res += str(i.value)
        return str_res


class SqlExtractor(BaseExtractor):
    """提取sql语句"""

    @staticmethod
    def get_full_name(identifier, including_dbs=False):
        if len(identifier.tokens) > 1 and identifier.tokens[1].value == '.':
            a = identifier.tokens[0].value
            b = identifier.tokens[2].value
            db_table = (a, b)
            full_tree = '{}.{}'.format(a, b)
            if len(identifier.tokens) == 3:
                return full_tree
            else:
                i = identifier.tokens[3].value
                c = identifier.tokens[4].value
                if i == ' ':
                    return full_tree
                full_tree = '{}.{}.{}'.format(a, b, c)
                return full_tree
        return None, None


if __name__ == '__main__':
    sql = """select
    b.product_name "产品",
    count(a.order_id) "订单量",
    b.selling_price_max "销售价",
    b.gross_profit_rate_max/100 "毛利率",
    case when b.business_type =1 then '自营消化' when b.business_type =2 then '服务商消化'  end "消化模式"
    from(select 'CRM签单' label,date(d.update_ymd) close_ymd,c.product_name,c.product_id,
        a.order_id,cast(a.recipient_amount as double) amt,d.cost
        from mysql4.dataview_fenxiao.fx_order a
        left join mysql4.dataview_fenxiao.fx_order_task b on a.order_id = b.order_id
        left join mysql7.dataview_trade.ddc_product_info c on cast(c.product_id as varchar) = a.product_ids and c.snapshot_version = 'SELLING'
        inner join (select t1.par_order_id,max(t1.update_ymd) update_ymd,
                    sum(case when t4.product2_type = 1 and t5.shop_id is not null then t5.price else t1.order_hosted_price end) cost
                   from hive.bdc_dwd.dw_mk_order t1
                   left join hive.bdc_dwd.dw_mk_order_status t2 on t1.order_id = t2.order_id and t2.acct_day = substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2)
                   left join mysql7.dataview_trade.mk_order_merchant t3 on t1.order_id = t3.order_id
                   left join mysql7.dataview_trade.ddc_product_info t4 on t4.product_id = t3.MERCHANT_ID and t4.snapshot_version = 'SELLING'
                   left join mysql4.dataview_scrm.sc_tprc_product_info t5 on t5.product_id = t4.product_id and t5.shop_id = t1.seller_id
                   where t1.acct_day = substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2)
                   and t2.valid_state in (100,200) ------有效订单
                   and t1.order_mode = 10    --------产品消耗订单
                   and t2.complete_state = 1  -----订单已经完成
                   group by t1.par_order_id
        ) d on d.par_order_id  = b.task_order_id
        where c.product_type = 0 and date(from_unixtime(a.last_recipient_time)) > date('2016-01-01') and a.payee_type <> 1 -----------已收款
        UNION ALL
        select '企业管家消耗' label,date(c.update_ymd) close_ymd,b.product_name,b.product_id,
        a.task_id,(case when a.yb_price = 0 and b.product2_type = 1 then b.selling_price_min else a.yb_price end) amt,
        (case when a.yb_price = 0 and b.product2_type = 2 then 0 when b.product2_type = 1 and e.shop_id is not null then e.price else c.order_hosted_price end) cost
        from mysql8.dataview_tprc.tprc_task a
        left join mysql7.dataview_trade.ddc_product_info b on a.product_id = b.product_id and b.snapshot_version = 'SELLING'
        inner join hive.bdc_dwd.dw_mk_order c on a.order_id = c.order_id and c.acct_day = substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2)
        left join hive.bdc_dwd.dw_mk_order_status d on d.order_id = c.order_id and d.acct_day = substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2)
        left join mysql4.dataview_scrm.sc_tprc_product_info e on e.product_id = b.product_id and e.shop_id = c.seller_id
        where  d.valid_state in (100,200) and d.complete_state = 1  and c.order_mode = 10
        union ALL
        select '交易管理系统' label,date(t6.close_ymd) close_ymd,t4.product_name,t4.product_id,
        t1.order_id,(t1.order_hosted_price-t1.order_refund_price) amt,
        (case when t1.order_mode <> 11 then t7.user_amount when t1.order_mode = 11 and t4.product2_type = 1 and t5.shop_id is not null then t5.price else t8.cost end) cost
        from hive.bdc_dwd.dw_mk_order t1
        left join hive.bdc_dwd.dw_mk_order_business t2 on t1.order_id = t2.order_id and t2.acct_day=substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2)
        left join mysql7.dataview_trade.mk_order_merchant t3 on t1.order_id = t3.order_id
        left join mysql7.dataview_trade.ddc_product_info t4 on t4.product_id = t3.MERCHANT_ID and t4.snapshot_version = 'SELLING'
        left join mysql4.dataview_scrm.sc_tprc_product_info t5 on t5.product_id = t4.product_id and t5.shop_id = t1.seller_id
        left join hive.bdc_dwd.dw_fact_task_ss_daily t6 on t6.task_id = t2.task_id and t6.acct_time=date_format(date_add('day',-1,current_date),'%Y-%m-%d')
        left join (select a.task_id,sum(a.user_amount) user_amount
                   from hive.bdc_dwd.dw_fn_deal_asyn_order a
                   where a.is_new=1 and a.service='Trade_Payment' and a.state=1 and a.acct_day=substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2)
                   group by a.task_id)t7 on t7.task_id = t2.task_id          
        left join (select t1.par_order_id,sum(t1.order_hosted_price - t1.order_refund_price) cost
                   from hive.bdc_dwd.dw_mk_order t1
                   where t1.acct_day = substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2) and t1.order_type = 1 and t1.order_stype = 4 and t1.order_mode = 12
                   group by t1.par_order_id) t8 on t1.order_id = t8.par_order_id
        where t1.acct_day = substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2)
        and t1.order_type = 1 and t1.order_stype in (4,5) and t1.order_mode <> 12 and t4.product_id is not null and t1.order_hosted_price > 0 and t6.is_deal = 1 and t6.close_ymd >= '2018-12-31'
    )a
    left join mysql7.dataview_trade.ddc_product_info b on a.product_id = b.product_id and b.snapshot_version = 'SELLING'
    where b.product2_type = 1 -------标品
    and close_ymd between DATE_ADD('day',-7,CURRENT_DATE)  and DATE_ADD('day',-1,CURRENT_DATE)
    GROUP BY b.product_name,
    b.selling_price_max,
    b.gross_profit_rate_max/100,
    b.actrul_supply_num,
    case when b.business_type =1 then '自营消化' when b.business_type =2 then '服务商消化'  end
    order by count(a.order_id) desc
    limit 10"""
    sql_extractor = SqlExtractor(sql)

    print(sql_extractor.sql)
    print(sql_extractor.tables)
