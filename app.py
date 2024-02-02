# app.py

from flask import Flask, request, jsonify
from flask_restful import Api, Resource, reqparse
from pymongo import MongoClient
from datetime import datetime
from celery import Celery
import json

app = Flask(__name__)
api = Api(app)

# MongoDB configuration
mongo_client = MongoClient('mongodb+srv://mani22kmk222:12345678mani@cluster0.jgwffnc.mongodb.net/?retryWrites=true&w=majority')
db = mongo_client['webhook_db']
webhooks_collection = db['webhooks']

# Import make_celery function from celery.py
from celery import Celery

def make_celery(app):
    celery = Celery(
        app.import_name,
        backend=app.config['CELERY_RESULT_BACKEND'],
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(app.config)
    return celery

# Configure Flask app for Celery
celery = make_celery(app)
logger = get_task_logger(__name__)

# Request parser for handling JSON input in requests
parser = reqparse.RequestParser()
parser.add_argument('company_id', type=str, required=True)
parser.add_argument('url', type=str, required=True)
parser.add_argument('headers', type=dict)
parser.add_argument('events', type=list, required=True)
parser.add_argument('is_active', type=bool, default=True)

# Resource for managing webhook subscriptions
class WebhooksResource(Resource):
    def get(self):
        # Get all webhooks for the user
        company_id = request.args.get('company_id')
        webhooks = list(webhooks_collection.find({'company_id': company_id}))
        return jsonify(webhooks)

    def post(self):
        # Create a new webhook subscription
        args = parser.parse_args()
        webhook = {
            'company_id': args['company_id'],
            'url': args['url'],
            'headers': args['headers'] or {},
            'events': args['events'],
            'is_active': args['is_active'],
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
        }
        webhooks_collection.insert_one(webhook)
        return jsonify(webhook)

# Resource for managing a specific webhook
class WebhookResource(Resource):
    def get(self, webhook_id):
        # Get a specific webhook by ID
        webhook = webhooks_collection.find_one({'_id': webhook_id})
        return jsonify(webhook)

    def patch(self, webhook_id):
        # Update a specific webhook by ID
        args = parser.parse_args()
        updated_data = {
            'url': args['url'],
            'headers': args['headers'] or {},
            'events': args['events'],
            'is_active': args['is_active'],
            'updated_at': datetime.utcnow(),
        }
        webhooks_collection.update_one({'_id': webhook_id}, {'$set': updated_data})
        return jsonify(updated_data)

    def delete(self, webhook_id):
        # Delete a specific webhook by ID
        webhooks_collection.delete_one({'_id': webhook_id})
        return {'message': 'Webhook deleted successfully'}

# Register resources with the API
api.add_resource(WebhooksResource, '/webhooks/')
api.add_resource(WebhookResource, '/webhooks/<ObjectId:webhook_id>')

# Celery task to handle webhook execution
@celery.task(bind=True, max_retries=3)
def execute_webhook(self, webhook_id, event_data):
    # Get the webhook details
    webhook = webhooks_collection.find_one({'_id': webhook_id})

    if webhook:
        url = webhook['url']
        headers = webhook['headers']

        try:
            # Implement logic to execute the webhook
            # Example: response = requests.post(url, headers=headers, json=event_data)
            logger.info(f"Webhook {webhook_id} executed successfully")
        except Exception as e:
            # Retry the task with exponential backoff
            logger.warning(f"Webhook {webhook_id} failed. Retrying... ({self.request.retries + 1} attempts)")
            raise self.retry(exc=e, countdown=2 ** self.request.retries)

# Endpoint to fire an event and trigger associated webhooks
@app.route('/fire-event', methods=['POST'])
def fire_event():
    # Implement logic to fire an event, check for active webhooks, and trigger them
    args = parser.parse_args()
    company_id = args['company_id']
    event_data = {'event_type': 'example_event'}

    # Get active webhooks for the company
    active_webhooks = list(webhooks_collection.find({'company_id': company_id, 'is_active': True}))

    # Trigger webhooks asynchronously using Celery
    for webhook in active_webhooks:
        execute_webhook.apply_async(args=[webhook['_id'], event_data])

    return {'message': 'Event fired successfully'}

if __name__ == '__main__':
    app.run(debug=True)
