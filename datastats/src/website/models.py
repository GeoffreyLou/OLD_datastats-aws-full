from django.db import models

class Job(models.Model):
    date_of_search = models.DateField()
    scrap_number = models.IntegerField()
    day_of_week = models.CharField(max_length=20)
    job_search = models.CharField(max_length=30)
    job_name = models.CharField(max_length=300)
    company_name = models.CharField(max_length=300)
    city_name = models.CharField(max_length=120)
    city = models.CharField(max_length=120)
    region = models.CharField(max_length=120)
    technos = models.TextField()
    description = models.TextField()
    lower_salary = models.FloatField()
    upper_salary = models.FloatField()
    job_type = models.CharField(max_length=120)
    sector = models.CharField(max_length=300)

    class Meta:
        db_table = 'jobs'


class Cloud(models.Model):
    cloud_count = models.IntegerField()

    class Meta:
        db_table = 'cloud_count'