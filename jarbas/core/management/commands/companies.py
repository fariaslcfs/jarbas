import csv
import lzma
import re
from functools import partial

from django.core.exceptions import ValidationError
from django.core.validators import validate_email

from jarbas.core.management.commands import LoadCommand
from jarbas.core.models import Company


class Command(LoadCommand):
    help = 'Load Serenata de Amor companies dataset into the database'

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '--batch-size', '-b', dest='batch_size',
            type=int, default=10000,
            help='Number of companies to be created at a time'
        )

    def handle(self, *args, **options):
        self.path = options['dataset']
        self.count = self.print_count(Company)
        print('Starting with {:,} companies'.format(self.count))

        if options.get('drop', False):
            self.drop_all(Company)
            self.count = 0

        self.bulk_create_by(self.companies, options['batch_size'])

    @property
    def companies(self):
        """
        Receives path to the dataset file and create a Company object for
        each row of each file. It creates the related activity when needed.
        """
        model_fields = list(f.name for f in Company._meta.fields)
        is_valid = partial(self.is_valid, model_fields)
        with lzma.open(self.path, mode='rt') as file_handler:
            for row in csv.DictReader(file_handler):
                keys = filter(is_valid, row.keys())
                filtered = {k: v for k, v in row.items() if k in keys}
                obj = Company(**self.serialize(filtered))

                yield obj

    def serialize_activities(self, row):
        row['main_activity'] = dict(
            code=int(row.get('main_activity_code', 0)),
            description=row.get('main_activity')
        )
        del(row['main_activity_code'])

        secondaries = []
        for num in range(1, 100):
            code = int(row.get('secondary_activity_{}_code'.format(num), 0))
            description = row.get('secondary_activity_{}'.format(num))
            del(row['secondary_activity_{}_code'.format(num)])
            del(row['secondary_activity_{}'.format(num)])
            if code and description:
                data = dict(code=code, description=description)
                secondaries.append(data)

            row['secondary_activity'] = secondaries

        return row

    def serialize(self, row):
        row['email'] = self.to_email(row['email'])

        dates = ('opening', 'situation_date', 'special_situation_date')
        for key in dates:
            row[key] = self.to_date(row[key])

        decimals = ('latitude', 'longitude')
        for key in decimals:
            row[key] = self.to_number(row[key])

        row = self.serialize_activities(row)

        return row

    @staticmethod
    def to_email(email):
        try:
            validate_email(email)
            return email

        except ValidationError:
            return None

    def bulk_create_by(self, companies, size):
        batch = list()
        for company in companies:
            batch.append(company)
            if len(batch) == size:
                self.bulk_create(batch)
                batch = list()
        self.bulk_create(batch)

    def bulk_create(self, batch):
        Company.objects.bulk_create(batch)
        self.count += len(batch)
        self.print_count(Company, count=self.count)

    def is_valid(self, fields, field):
        if field == 'secondary_activity':
            return False

        if field == 'main_activity_code':
            return True

        if field in fields:
            return True

        regex = re.compile(r'secondary_activity_([\d]{1,2})(_code)?')
        return bool(regex.match(field))
