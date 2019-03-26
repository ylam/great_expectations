import sqlalchemy as sa
import great_expectations as ge
from great_expectations.dataset import SqlAlchemyDataset, MetaSqlAlchemyDataset
from pprint import pprint
import pandas as pd

class CustomSqlAlchemyDataset(SqlAlchemyDataset):
    # @MetaSqlAlchemyDataset.column_aggregate_expectation
    # def expect_column_id_to_equal_1(self, column):
    #     query = sa.select([sa.column(column).label('value')]).select_from(self._table)

    #     column_value = self.engine.execute(query).scalar()
    #     return {
    #         "success": column_value == 1,
    #         "result": {
    #             "observed_value": column_value
    #         }
    #     }

    # @MetaSqlAlchemyDataset.column_aggregate_expectation
    # def expect_column_mode_to_equal_0(self, column):
    #     mode_query = sa.select([
    #         sa.column(column).label('value'),
    #         sa.func.count(sa.column(column)).label('frequency')
    #     ]).select_from(self._table).group_by(sa.column(column)).order_by(
    #         sa.desc(sa.column('frequency')))

    #     mode = self.engine.execute(mode_query).scalar()
    #     return {
    #         "success": mode == 0,
    #         "result": {
    #             "observed_value": mode,
    #         }
    #     }

    @MetaSqlAlchemyDataset.column_aggregate_expectation
    def expect_column_name_to_equal_pythonroad(self, column):
        query = sa.select([sa.column(column)]).select_from(self._table)

        column_value = self.engine.execute(query).scalar()
        return {
            "success": column_value == 'python road',
            "result": {
                "observed_value": column_value
            }
        }

def custom_dataset():
    engine = sa.create_engine('sqlite:///example.db')
    query = '''
        select p.id, p.name, a.street_name
        from person as p 
            inner join address as a
            on p.id = a.id
    '''
    
    # Error
    # custom_dataset = CustomSqlAlchemyDataset(table_name='person', engine=engine, custom_sql=query)
    
    # No error
    custom_dataset = CustomSqlAlchemyDataset(table_name='address', engine=engine, custom_sql=query)
    # custom_dataset.create_temporary_table(table_name='temp_table', custom_sql=query)

    return custom_dataset

def test_custom_sqlalchemydataset(custom_dataset):
    custom_dataset._initialize_expectations()
    custom_dataset.set_default_expectation_argument(
        "result_format", {"result_format": "COMPLETE"})

    # result = custom_dataset.expect_column_id_to_equal_1('id')
    # assert result['success'] == True
    # assert result['result']['observed_value'] == 1

    pprint(custom_dataset.columns)
    result = custom_dataset.expect_column_name_to_equal_pythonroad('street_name')
    assert result['success'] == True
    assert result['result']['observed_value'] == 'python road'

def test_sqlalchemydataset_with_custom_sql_2():
    engine = sa.create_engine('sqlite:///example.db')

    # data = pd.DataFrame({
    #     "name": ["Frank", "Steve", "Jane", "Michael"],
    #     "age": [16, 21, 38, 10],
    #     "pet": ["fish", "python", "cat", "frog"]
    # })

    # data2 = pd.DataFrame({
    #     "name": ["Frank", "Steve", "Jane", "Michael"],
    #     "weight": [10, 10, 20, 10]
    # })

    # data.to_sql(name='test_sql_data', con=engine, index=False)
    # data2.to_sql(name='test_sql_data2', con=engine, index=False)

    # custom_sql = '''
    #     select a.*
    #     from person as p 
    #         inner join address as a
    #         on p.id = a.id
    # '''

    # with engine.connect() as connection:
    #     try:
    #         result = connection.execute(custom_sql)
    #     except Exception as e:
    #         print(e)
    #     else:
    #         for row in result:
    #             print(row)

    custom_sql = "SELECT d2.weight FROM test_sql_data as d1 inner join test_sql_data2 as d2 on d1.name = d2.name WHERE age > 12 and weight = 10"

    custom_sql_dataset = CustomSqlAlchemyDataset(
        'test_sql_data_3', engine=engine, custom_sql=custom_sql)

    custom_sql_dataset._initialize_expectations()
    custom_sql_dataset.set_default_expectation_argument(
        "result_format", {"result_format": "COMPLETE"})

    result = custom_sql_dataset.expect_column_to_exist("weight")
    assert result['success'] == True

    result = custom_sql_dataset.expect_column_to_exist("pet")
    assert result['success'] == False

def test_sqlalchemydataset_with_custom_sql():
    engine = sa.create_engine('sqlite://')

    data = pd.DataFrame({
        "name": ["Frank", "Steve", "Jane", "Frank", "Michael"],
        "age": [16, 21, 38, 22, 10],
        "pet": ["fish", "python", "cat", "python", "frog"]
    })

    data.to_sql(name='test_sql_data', con=engine, index=False)

    custom_sql = "SELECT name, pet FROM test_sql_data WHERE age > 25"
    custom_sql_dataset = SqlAlchemyDataset(
        'test_sql_data', engine=engine, custom_sql=custom_sql)

    custom_sql_dataset._initialize_expectations()
    custom_sql_dataset.set_default_expectation_argument(
        "result_format", {"result_format": "COMPLETE"})

    result = custom_sql_dataset.expect_column_values_to_be_in_set(
        "pet", ["fish", "cat", "python"])
    assert result['success'] == True

    result = custom_sql_dataset.expect_column_to_exist("age")
    assert result['success'] == False

def test_custom_sql():
    options = 'sqlite:///example.db'
    sql_context = ge.get_data_context('SqlAlchemy', options)
    pprint(sql_context.list_datasets())
    
    query = '''
        select a.*
        from person as p 
            inner join address as a
            on p.id = a.id
    '''
    sql_dataset = sql_context.get_dataset(dataset_name='person', custom_sql=query)
    
    # sql_dataset.expect_column_values_to_not_be_null('id')
    # Fails here because something wrong with dataset_name
    sql_dataset.expect_column_values_to_not_be_null('street_name')

    # pprint(sql_dataset.validate())

if __name__ == "__main__":
    # Attempt 1
    # test_custom_sql()

    # Attempt 2
    custom_dataset = custom_dataset()
    test_custom_sqlalchemydataset(custom_dataset)

    # Attempt 3 with their test logic pushing pandas dataframe data into a sql table
    # test_sqlalchemydataset_with_custom_sql()

    # Attempt 4 - failed as I can't use custom sql
    # test_sqlalchemydataset_with_custom_sql_2()