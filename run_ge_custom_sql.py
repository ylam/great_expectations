import sqlalchemy as sa
import great_expectations as ge
from great_expectations.dataset import SqlAlchemyDataset, MetaSqlAlchemyDataset

class CustomSqlAlchemyDataset(SqlAlchemyDataset):
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
    custom_dataset = CustomSqlAlchemyDataset(table_name='person', engine=engine, custom_sql=query)   

    return custom_dataset

def test_custom_sqlalchemydataset(custom_dataset):
    custom_dataset._initialize_expectations()
    custom_dataset.set_default_expectation_argument(
        "result_format", {"result_format": "COMPLETE"})

    result = custom_dataset.expect_column_name_to_equal_pythonroad('street_name')
    assert result['success'] == True
    assert result['result']['observed_value'] == 'python road'

if __name__ == "__main__":
    custom_dataset = custom_dataset()
    test_custom_sqlalchemydataset(custom_dataset)