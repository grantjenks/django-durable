from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("django_durable", "0005_child_workflows"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="workflowexecution",
            index=models.Index(fields=["status", "updated_at"], name="wf_status_updated_idx"),
        ),
        migrations.AddIndex(
            model_name="workflowexecution",
            index=models.Index(fields=["status", "expires_at"], name="wf_status_expires_idx"),
        ),
        migrations.AddIndex(
            model_name="historyevent",
            index=models.Index(fields=["execution", "pos", "type"], name="he_exec_pos_type_idx"),
        ),
        migrations.AddIndex(
            model_name="historyevent",
            index=models.Index(fields=["execution", "type", "id"], name="he_exec_type_id_idx"),
        ),
        migrations.AddIndex(
            model_name="activitytask",
            index=models.Index(fields=["status", "expires_at"], name="at_status_expires_idx"),
        ),
        migrations.AddIndex(
            model_name="activitytask",
            index=models.Index(fields=["status", "heartbeat_timeout"], name="at_status_hb_idx"),
        ),
        migrations.AddIndex(
            model_name="activitytask",
            index=models.Index(fields=["status", "updated_at"], name="at_status_updated_idx"),
        ),
    ]
