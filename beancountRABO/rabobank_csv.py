#!/usr/bin/env python3
""" Importer for CSV statements from RABO Bank"""

__copyright__ = "Copyright (C) 2020 Elise van Wijngaarden"
__license__ = "GPLv2"

import csv
import os
import re
import string
import sys
from datetime import timedelta
from dateutil.parser import parse

import pandas as pd

from beancount.core import data,flags,amount
from beancount.core.number import D
from beancount.core.position import CostSpec
from beancount.ingest import importer


class RABOImporter(importer.ImporterProtocol):
    """An importer for RABO Bank CSV files"""

    def __init__(self, account_root, account_no, payee_map_file):
        self.account_root = account_root
        self.account_no = account_no
        self.payee_map_file = payee_map_file

    def name(self):
        return "RABO Bank CSV Importer"

    def identify(self, file):
        filename = os.path.basename(file.name)
        return re.match('CSV_A_\\d{8}_\\d{6}\\.csv', filename) 

    def file_name(self, file):
        return 'rabo_{}'.format(os.path.basename(file.name))

    def file_account(self, _):
        return self.account_root

    def file_date(self, file):
        return parse(os.path.basename(file.name).split('_')[0], dayfirst=True).date()

    def extract(self, file, existing_entries=None):
        try:
            payee_df = pd.read_csv(self.payee_map_file, sep=',', header=0,
                    index_col=0, keep_default_na=False)
        except IOError:
            payee_df = pd.DataFrame(columns=['RAW', 'BC', 'POSTING'])
            print("Writing to new cache {}".format(self.payee_map_file), file=sys.stderr)
        new_payees = {}
        entries = []
        index = 0
        row = {}
        with open(file.name) as file_open:
            for index, row in enumerate(csv.DictReader(file_open)):
                payee = string.capwords(row['Naam tegenpartij'])
                narration = row['Omschrijving-1']
                if re.match("^'.*'$", narration):
                    narration = narration[1:-1]
                if re.match('\\s*', payee) and re.match('.*>.*', narration):
                    splt = narration.split('>',1)
                    payee = splt[0]
                    narration = splt[1]
                narration = re.sub("\\s+", " ", narration).strip()
                payee = re.sub("\\s+", " ", payee).strip()
                payee_mpd = map_payee(payee_df, new_payees, payee, row)
                if payee_mpd == "\0":
                    index-=1
                    break

                txn = data.Transaction(
                        meta = data.new_metadata(file.name, index),
                        date = parse(row['Datum'], yearfirst=False, dayfirst=False).date(),
                        flag = flags.FLAG_OKAY,
                        payee = payee_mpd if payee_mpd else None,
                        narration = narration,
                        tags = set(),
                        links = set(),
                        postings =[],
                        )


                txn.postings.append(
                        data.Posting(self.account_root,
                            amount.Amount(D(row['Bedrag'].replace(',','.')),row['Munt']), CostSpec(None, row['Oorspr bedrag'], row['Oorspr munt'], None, None, None), None, None, None)
                        )
                add_post(txn, payee_df, payee, row)
                entries.append(txn)

        if index:
            entries.append(
                    data.Balance(
                        data.new_metadata(file.name, index),
                        parse(row['Datum'], dayfirst=True).date() + timedelta(days=1),
                        self.account_root,
                        amount.Amount(D(row['Saldo na trn']), row['Munt']),
                        None,None
                        )
                    )

        if new_payees:
            new_payees_df = pd.DataFrame(new_payees.items(), columns=['RAW','BC'])
            payee_df.to_csv(self.payee_map_file + ".old")
            payee_df.append(new_payees_df, ignore_index=True).to_csv(self.payee_map_file)
        return entries

def map_payee(payee_df, new_payees, payee:str, row) -> str:
    """
    Refactors the payee using the payees cache. Prompts for
    a new name if payee is not found in the cache.
    """
    if row['Tegenrekening IBAN/BBAN']:
        key = row['Tegenrekening IBAN/BBAN']
    elif payee:
        key = payee
    else:
        key = row['Omschrijving-1']
    # Check cache from payee_map_file
    ret = payee_df.loc[payee_df.RAW == key, 'BC']
    if not ret.empty:
        return ret.iloc[0]
    # Check cache with new_payees
    if key in new_payees:
        return new_payees[key]
    # Not found. Prompt for new payee name
    print("New payee in transaction\n"
            "Date: {}\n"
            "Payee: {}\n"
            "Account: {}\n"
            "Amount: {}{}\n"
            "Narration: {}\n"
            "Give a name for {}, = to preserve, q to exit, or s to skip."\
                    .format(
                        row['Datum'],
                        payee,
                        row['Tegenrekening IBAN/BBAN'],
                        row['Munt'],
                        row['Bedrag'],
                        row['Omschrijving-1'],
                        key
                        ),
                    file=sys.stderr
                    )
    value = input()
    # Return payee depending on returned value
    if value == "s":
        return payee
    if value == "q":
        return "\0"
    if value == "=":
        value = payee
    if not key:
        return value
    new_payees[key] = value
    print("Adding {} -> {}".format(key, value), file=sys.stderr)
    return value

def add_post(txn, payee_df, payee, row) -> None:
    """
    Adds new postings to txn based on data in payee_df or row
    """
    if row['Tegenrekening IBAN/BBAN']:
        key = row['Tegenrekening IBAN/BBAN']
    elif payee:
        key = payee
    else:
        key = row['Omschrijving-1']
    # Check cache from payee_map_file
    ret = payee_df.loc[payee_df.RAW == key, 'POSTING']
    if not ret.empty and ret.iloc[0]:
        txn.postings.append(
                data.Posting(str(ret.iloc[0]),
                    None, None, None, None, None)
                )
