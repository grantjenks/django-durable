from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("django_durable", "0004_activity_heartbeat"),
    ]

    operations = [
        migrations.AddField(
            model_name="workflowexecution",
            name="parent",
            field=models.ForeignKey(
                to="django_durable.workflowexecution",
                on_delete=django.db.models.deletion.CASCADE,
                related_name="children",
                null=True,
                blank=True,
            ),
        ),
        migrations.AddField(
            model_name="workflowexecution",
            name="parent_pos",
            field=models.IntegerField(null=True, blank=True),
        ),
    ]
