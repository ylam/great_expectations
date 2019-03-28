import sqlalchemy as sa
import great_expectations as ge
from great_expectations.dataset import SqlAlchemyDataset, MetaSqlAlchemyDataset
from pprint import pprint

def custom_dataset():
    engine = sa.create_engine('sqlite:///ge_custom_sql.db')
    query = '''
        select p.id, p.name, a.street_name
        from person as p 
            inner join address as a
            on p.id = a.id
    '''
    
    # Error
    dataset = SqlAlchemyDataset(table_name='person', engine=engine, custom_sql=query)   

    return dataset

def test_custom_sqlalchemydataset(custom_dataset):

    print('Columns from custom dataset ')
    pprint(custom_dataset.columns)

    print('Expect column from custom sql')
    result = custom_dataset.expect_column_to_exist('street_name')
    assert result['success'] == True
    assert result['result']['observed_value'] == 'python road'

if __name__ == "__main__":
    custom_dataset = custom_dataset()
    test_custom_sqlalchemydataset(custom_dataset)