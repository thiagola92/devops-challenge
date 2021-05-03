import mysql.connector
from datetime import datetime

class Database():
    def __init__(self, *args, **kwargs):
        self.db_args = args
        self.db_kwargs = kwargs

    def get_product(self, product_id, fields):
        include_barcodes = fields.get('barcodes')
        include_attributes = fields.get('attributes')
        
        product = {}

        try:
            connection = mysql.connector.connect(*self.db_args, **self.db_kwargs)
            cursor = connection.cursor()

            fields_qty = len([f for f in fields.keys() if f not in ['barcodes', 'attributes']])

            if fields_qty > 0:
                cursor.execute(f"SELECT * FROM nodis_devops_test.product WHERE product_id = %s", (product_id, ))
                row = cursor.fetchone()
                columns = [d[0] for d in cursor.description]

                for c, r in zip(columns, row):
                    if fields.get(c):
                        product[c] = r

            if include_barcodes:
                cursor.execute("SELECT DISTINCT(barcode) FROM nodis_devops_test.product_barcode WHERE product_id = %s", (product_id, ))
                product['barcodes'] = []
                
                for m in cursor.fetchall():
                    product['barcodes'].append(m[0])

            if include_attributes:
                cursor.execute("SELECT name, value FROM nodis_devops_test.product_attribute WHERE product_id = %s", (product_id, ))
                product['attributes'] = []
                
                for m in cursor.fetchall():
                    product['attributes'].append({'name': m[0], 'value': m[1]})
            
            connection.commit()
        finally:
            try:
                cursor.close()
            except:
                pass

            try:
                connection.close()
            except:
                pass
        
        return product


    def insert_product(self, product):
        try:
            connection = mysql.connector.connect(*self.db_args, **self.db_kwargs)
            cursor = connection.cursor()

            cursor.execute("""
                INSERT INTO nodis_devops_test.product (
                    title,
                    sku,
                    description,
                    price,
                    created,
                    last_updated
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                product['title'],
                product['sku'],
                product['description'],
                product['price'],
                datetime.utcnow().isoformat(),
                datetime.utcnow().isoformat()
            ))

            cursor.execute("SELECT LAST_INSERT_ID()")

            product_id = cursor.fetchone()[0]

            for barcode in product['barcodes']:
                cursor.execute("""
                    INSERT INTO nodis_devops_test.product_barcode (
                        product_id,
                        barcode
                    ) VALUES (%s, %s)
                """, (
                    product_id,
                    barcode
                ))

            for attribute in product['attributes']:
                cursor.execute("""
                    INSERT INTO nodis_devops_test.product_attribute (
                        product_id,
                        name,
                        value
                    ) VALUES (%s, %s, %s)
                """, (
                    product_id,
                    attribute['name'],
                    attribute['value']
                ))
            
            connection.commit()
        finally:
            try:
                cursor.close()
            except:
                pass
            
            try:
                connection.close()
            except:
                pass
        
        return product_id

    def put_product(self, changes):
        fields = {f: 1 for f in changes.keys() if f != 'product_id'}
        product = self.get_product(changes['product_id'], fields)
        product.update(changes)

        try:
            connection = mysql.connector.connect(*self.db_args, **self.db_kwargs)
            cursor = connection.cursor()

            cursor.execute("""
                UPDATE nodis_devops_test.product
                SET
                    title = %s,
                    sku = %s,
                    description = %s,
                    price = %s,
                    last_updated = %s
                WHERE
                    product_id = %s
            """, (
                product['title'],
                product['sku'],
                product['description'],
                product['price'],
                datetime.utcnow().isoformat(),
                product['product_id']
            ))

            # for barcode in product['barcodes']:
            #     cursor.execute("""
            #         UPDATE nodis_devops_test.product_barcode
            #         SET
            #             barcode = %s
            #         WHERE
            #             product_id = %s
            #     """, (
            #         product_id,
            #         barcode
            #     ))

            # for attribute in product['attributes']:
            #     cursor.execute("""
            #         UPDATE nodis_devops_test.product_attribute
            #         SET
            #             value = %s
            #         WHERE
            #             name = %s
            #             AND product_id = %s
            #     """, (
            #         attribute['value'],
            #         attribute['name'],
            #         product_id
            #     ))
            
            connection.commit()
        finally:
            try:
                cursor.close()
            except:
                pass

            try:
                connection.close()
            except:
                pass
        
        return 'true'

    def delete_product(self, product_id):
        try:
            connection = mysql.connector.connect(*self.db_args, **self.db_kwargs)
            cursor = connection.cursor()

            cursor.execute("DELETE FROM nodis_devops_test.product WHERE product_id = %s", (product_id, ))
            cursor.execute("DELETE FROM nodis_devops_test.product_barcode WHERE product_id = %s", (product_id, ))
            cursor.execute("DELETE FROM nodis_devops_test.product_attribute WHERE product_id = %s", (product_id, ))
            
            connection.commit()
        finally:
            try:
                cursor.close()
            except:
                pass

            try:
                connection.close()
            except:
                pass
        
        return 'true'