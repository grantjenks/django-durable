from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("django_durable", "0003_timeouts_and_retry"),
    ]

    operations = [
        migrations.AddField(
            model_name="activitytask",
            name="heartbeat_timeout",
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="activitytask",
            name="heartbeat_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="activitytask",
            name="heartbeat_details",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]

