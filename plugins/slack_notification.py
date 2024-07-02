def notification(context, message: str = None):

    from airflow.hooks.base_hook import BaseHook
    from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

    if context and message is None:

        # MENSAGEM ENVIADA NO CANAL DO SLACK
        message = f"""
            :red_circle: Task failed
            *Task:* {context.get('task_instance').task_id}
            *Dag:* {context.get('task_instance').dag_id}
            *Execution time:* {context.get('execution_date')}
            *Log URL:* {context.get('task_instance').log_url}
        """

        # BUSCA CONEXAO DO SLACK NO AIRFLOW
        slack_token = BaseHook.get_connection('slack_connection').password

        # INSTANCIA O SLACK WEBHOOK
        notification = SlackWebhookOperator(
            task_id='notification',
            http_conn_id='slack_abinbev',
            webhook_token=slack_token,
            message=message,
            username='airflow'
        )

        # RETORNA MENSAGEM DE ERRO DADO O CONTEXTO DA DAG
        return notification.execute(context=context)