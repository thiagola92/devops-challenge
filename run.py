import re
import time
import asyncio
from quart import Quart, request

from database import Database

database = Database(user='root', password='password', host='172.18.0.3')
app = Quart(__name__)

@app.route('/')
async def index():
    return 'index'

@app.route('/api/products/<product_id>')
async def get_product_id(product_id):
    product_id = int(product_id)
    fields = request.args.get('fields')
    fields = fields.split(',') if fields else []
    fields = [re.sub('\s', '', f) for f in fields]
    fields = {f: True for f in fields}
    return database.get_product(product_id, fields)

@app.route('/api/products', methods=['POST'])
async def post_products():
    product = await request.get_json()
    product_id = database.insert_product(product)
    return str(product_id)

@app.route('/api/products', methods=['PUT'])
async def put_products():
    product = await request.get_json()
    return database.put_product(product)

@app.route('/api/products/<product_id>', methods=['DELETE'])
async def delete_product_id(product_id):
    product_id = int(product_id)
    return database.delete_product(product_id)

if __name__ == '__main__':
    app.run()