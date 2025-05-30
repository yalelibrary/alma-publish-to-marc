import boto3

'''
   Usage:

   # send single metric
   send_metric_count('datasync', 'ingest', 50, 'environment', 'prod')

   # sending multiple metrics in a single call
   send_metric('datasync', [{'name': 'files', 'value': 2}, {'name': 'ingest', 'value': 50}, {'name': 'delete', 'value': 3}, {'name': 'error', 'value': 1}], 'environment', 'prod')

'''
def send_metric_count(namespace, name, count, dimension_name, dimension_value):
    metric_datum = {
                'MetricName': name,
                'Value': count,
                'Unit': 'Count'
            }
    send_metric(namespace, [metric_datum], dimension_name, dimension_value)


def send_metric(namespace, metric_counts, dimension_name, dimension_value):
    client = boto3.client('cloudwatch')
    metric_data = [{'MetricName': m['name'], 'Value': m['value'], 'Unit': m.get('unit', 'Count')} for m in metric_counts]
    if dimension_name and dimension_value:
         for m in metric_data:
            m['Dimensions'] = [{'Name': dimension_name, 'Value': dimension_value}]
    client.put_metric_data(
        Namespace=namespace,
        MetricData=metric_data)
