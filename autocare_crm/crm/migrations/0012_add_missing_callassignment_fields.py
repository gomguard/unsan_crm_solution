# crm/migrations/0012_add_missing_callassignment_fields.py
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('crm', '0011_callassignment'),
    ]

    operations = [
        migrations.AddField(
            model_name='callassignment',
            name='due_date',
            field=models.DateField(blank=True, null=True, verbose_name='처리기한'),
        ),
        migrations.AddField(
            model_name='callassignment',
            name='priority',
            field=models.CharField(choices=[('urgent', '긴급'), ('high', '높음'), ('normal', '보통'), ('low', '낮음')], default='normal', max_length=10),
        ),
        migrations.AlterField(
            model_name='callassignment',
            name='notes',
            field=models.TextField(blank=True, default='', verbose_name='배정메모'),
        ),
    ]