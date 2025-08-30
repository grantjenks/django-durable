from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("django_durable", "0002_rename_not_before_after_time"),
    ]

    operations = [
        migrations.AddField(
            model_name="workflowexecution",
            name="expires_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="activitytask",
            name="expires_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="activitytask",
            name="retry_policy",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
