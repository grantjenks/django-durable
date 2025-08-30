from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("django_durable", "0001_initial"),
    ]

    operations = [
        migrations.RenameField(
            model_name="activitytask",
            old_name="not_before",
            new_name="after_time",
        ),
        migrations.RemoveIndex(
            model_name="activitytask",
            name="django_dura_status_4e8fad_idx",
        ),
        migrations.AddIndex(
            model_name="activitytask",
            index=models.Index(
                fields=["status", "after_time"], name="django_dura_status_af_idx"
            ),
        ),
    ]

