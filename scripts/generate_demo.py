# Copyright 2026 Lamella LLC
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai

"""Generate the public demo ledger at ``ledger.demo/``.

Builds a complete, bean-check-clean Lamella ledger for a single
synthetic user (John Smith) with one business (Acme Co.), a
personal vehicle, a home with a 15-year-old mortgage, and a full
year (2025) of realistic activity. The demo respects:

* LEDGER_LAYOUT.md (§3.2 main.bean shape, §2.1 connector files)
* CHART_OF_ACCOUNTS.md (entity-first hierarchy, vehicle/property
  subtrees, Schedule A / Schedule C category names)
* ADR-0001 (ledger as source of truth)
* ADR-0003 (lamella-* metadata namespace)
* ADR-0007 (entity-first hierarchy)
* ADR-0017 (placeholder data only — Acme Co., John Smith,
  generic merchant names; no real merchants or real PII)
* ADR-0019 (paired source meta on bank-side posting)

All names are fictional. Numbers vary on a fixed RNG seed so
runs are reproducible. Re-run safely; the script wipes the
output directory before regenerating.

Usage::

    python scripts/generate_demo.py [--out ledger.demo/]
"""
from __future__ import annotations

import argparse
import calendar
import csv
import random
import shutil
import uuid
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

# ---------------------------------------------------------------
# Demo profile (placeholders only — see ADR-0017)
# ---------------------------------------------------------------

DEMO_YEAR = 2025
PERSONAL_ENTITY = "Personal"
BUSINESS_ENTITY = "Acme"
BANK_INST = "BankOne"

# Accounts
PERS_CHECKING = f"Assets:{PERSONAL_ENTITY}:{BANK_INST}:Checking"
PERS_SAVINGS = f"Assets:{PERSONAL_ENTITY}:{BANK_INST}:Savings"
PERS_CARD = f"Liabilities:{PERSONAL_ENTITY}:{BANK_INST}:Card"
ACME_CHECKING = f"Assets:{BUSINESS_ENTITY}:{BANK_INST}:Checking"
ACME_CARD = f"Liabilities:{BUSINESS_ENTITY}:{BANK_INST}:Card"

# Property + Vehicle
HOUSE_SLUG = "MainHouse"
HOUSE_ASSET = f"Assets:{PERSONAL_ENTITY}:Properties:{HOUSE_SLUG}"
HOUSE_PROPERTY_TAX = f"Expenses:{PERSONAL_ENTITY}:Properties:{HOUSE_SLUG}:PropertyTax"
HOUSE_INSURANCE = f"Expenses:{PERSONAL_ENTITY}:Properties:{HOUSE_SLUG}:Insurance"
HOUSE_MAINTENANCE = f"Expenses:{PERSONAL_ENTITY}:Properties:{HOUSE_SLUG}:Maintenance"
HOUSE_REPAIRS = f"Expenses:{PERSONAL_ENTITY}:Properties:{HOUSE_SLUG}:Repairs"
HOUSE_UTILITIES = f"Expenses:{PERSONAL_ENTITY}:Properties:{HOUSE_SLUG}:Utilities"
HOUSE_HOA = f"Expenses:{PERSONAL_ENTITY}:Properties:{HOUSE_SLUG}:HOA"
HOUSE_MORT_INTEREST = (
    f"Expenses:{PERSONAL_ENTITY}:Properties:{HOUSE_SLUG}:MortgageInterest"
)

MORTGAGE_SLUG = "Mortgage2010"
MORTGAGE_LIABILITY = (
    f"Liabilities:{PERSONAL_ENTITY}:{BANK_INST}:Loans:{MORTGAGE_SLUG}"
)
MORTGAGE_ESCROW = (
    f"Expenses:{PERSONAL_ENTITY}:{BANK_INST}:Loans:{MORTGAGE_SLUG}:Escrow"
)

VEHICLE_SLUG = "V2018Sedan"
VEH_ASSET = f"Assets:{PERSONAL_ENTITY}:Vehicles:{VEHICLE_SLUG}"
VEH_FUEL = f"Expenses:{PERSONAL_ENTITY}:Vehicles:{VEHICLE_SLUG}:Fuel"
VEH_OIL = f"Expenses:{PERSONAL_ENTITY}:Vehicles:{VEHICLE_SLUG}:Oil"
VEH_TIRES = f"Expenses:{PERSONAL_ENTITY}:Vehicles:{VEHICLE_SLUG}:Tires"
VEH_INSURANCE = f"Expenses:{PERSONAL_ENTITY}:Vehicles:{VEHICLE_SLUG}:Insurance"
VEH_MAINTENANCE = f"Expenses:{PERSONAL_ENTITY}:Vehicles:{VEHICLE_SLUG}:Maintenance"
VEH_REGISTRATION = (
    f"Expenses:{PERSONAL_ENTITY}:Vehicles:{VEHICLE_SLUG}:Registration"
)
VEH_PARKING = f"Expenses:{PERSONAL_ENTITY}:Vehicles:{VEHICLE_SLUG}:Parking"

# Personal income + expense categories (Schedule A flavor)
PERS_INCOME_SALARY = f"Income:{PERSONAL_ENTITY}:Salary"
PERS_INCOME_INTEREST = f"Income:{PERSONAL_ENTITY}:Interest:{BANK_INST}"
PERS_GROCERIES = f"Expenses:{PERSONAL_ENTITY}:Groceries"
PERS_RESTAURANTS = f"Expenses:{PERSONAL_ENTITY}:Restaurants"
PERS_HEALTHCARE = f"Expenses:{PERSONAL_ENTITY}:Healthcare"
PERS_PHONE = f"Expenses:{PERSONAL_ENTITY}:Phone"
PERS_INTERNET = f"Expenses:{PERSONAL_ENTITY}:Internet"
PERS_ENTERTAINMENT = f"Expenses:{PERSONAL_ENTITY}:Entertainment"
PERS_CHARITY = f"Expenses:{PERSONAL_ENTITY}:Charity"  # Schedule A line 11
PERS_HOUSEHOLD = f"Expenses:{PERSONAL_ENTITY}:Household"
PERS_CLOTHING = f"Expenses:{PERSONAL_ENTITY}:Clothing"
PERS_TAX_FED = f"Expenses:{PERSONAL_ENTITY}:Tax:Federal"  # withheld via paycheck
PERS_TAX_STATE = f"Expenses:{PERSONAL_ENTITY}:Tax:State"
PERS_TAX_FICA = f"Expenses:{PERSONAL_ENTITY}:Tax:FICA"
PERS_BANK_FEES = f"Expenses:{PERSONAL_ENTITY}:Bank:{BANK_INST}:Fees"

# Acme business income + expense categories (Schedule C flavor)
ACME_INCOME_CONSULTING = f"Income:{BUSINESS_ENTITY}:Consulting"
ACME_INCOME_PRODUCT = f"Income:{BUSINESS_ENTITY}:Product"
ACME_ADVERTISING = f"Expenses:{BUSINESS_ENTITY}:Advertising"  # Sched C line 8
ACME_OFFICE = f"Expenses:{BUSINESS_ENTITY}:Office"  # line 18
ACME_SUPPLIES = f"Expenses:{BUSINESS_ENTITY}:Supplies"  # line 22
ACME_PROFESSIONAL = f"Expenses:{BUSINESS_ENTITY}:ProfessionalFees"  # line 17
ACME_SOFTWARE = f"Expenses:{BUSINESS_ENTITY}:Software"  # line 18 / 22
ACME_TELEPHONE = f"Expenses:{BUSINESS_ENTITY}:Telephone"  # line 25
ACME_INSURANCE = f"Expenses:{BUSINESS_ENTITY}:Insurance"  # line 15
ACME_MEALS = f"Expenses:{BUSINESS_ENTITY}:Meals"  # line 24b
ACME_TRAVEL = f"Expenses:{BUSINESS_ENTITY}:Travel"  # line 24a
ACME_BANK_FEES = f"Expenses:{BUSINESS_ENTITY}:Bank:{BANK_INST}:Fees"
ACME_VEHICLE_MILEAGE = (
    f"Expenses:{BUSINESS_ENTITY}:Vehicles:{VEHICLE_SLUG}:Mileage"
)

# Mileage offset (Equity per CHART_OF_ACCOUNTS.md "Vehicles" subtree)
ACME_MILEAGE_DEDUCTION = (
    f"Equity:{BUSINESS_ENTITY}:Vehicles:{VEHICLE_SLUG}:MileageDeductions"
)

# Equity opening balances
PERS_OPENING = f"Equity:{PERSONAL_ENTITY}:OpeningBalances"
ACME_OPENING = f"Equity:{BUSINESS_ENTITY}:OpeningBalances"

# Project: tagged via metadata project: "ProjectAlpha"
PROJECT_NAME = "ProjectAlpha"

# IRS standard mileage rate for 2025 (placeholder; the real number
# is 70¢/mi but we'll use 0.67 like 2024 to stay clearly synthetic).
MILEAGE_RATE = Decimal("0.67")


# ---------------------------------------------------------------
# Small Beancount writers
# ---------------------------------------------------------------

def fmt_amount(amount: Decimal | str | int | float, currency: str = "USD") -> str:
    if not isinstance(amount, Decimal):
        amount = Decimal(str(amount))
    return f"{amount:.2f} {currency}"


def fmt_money(amount: Decimal | str | int | float) -> str:
    if not isinstance(amount, Decimal):
        amount = Decimal(str(amount))
    return f"{amount:.2f}"


def mint_uuid() -> str:
    """UUIDv4 stand-in. Real Lamella mints UUIDv7 for time-orderability;
    a v4 round-trips fine through the loader and is deterministic
    when the global RNG is seeded."""
    return str(uuid.UUID(int=random.getrandbits(128), version=4))


def q(s: str | None) -> str:
    return (s or "").replace('"', "\\\"")


@dataclass
class Posting:
    account: str
    amount: Decimal | None  # None → elided
    currency: str = "USD"
    meta: list[tuple[str, str]] = field(default_factory=list)

    def render(self) -> str:
        if self.amount is None:
            head = f"  {self.account}"
        else:
            head = f"  {self.account:<60s} {self.amount:>10.2f} {self.currency}"
        lines = [head]
        for k, v in self.meta:
            lines.append(f"    {k}: {v}")
        return "\n".join(lines)


@dataclass
class Txn:
    date: date
    payee: str | None
    narration: str
    postings: list[Posting]
    flag: str = "*"
    tags: list[str] = field(default_factory=list)
    links: list[str] = field(default_factory=list)
    meta: list[tuple[str, str]] = field(default_factory=list)

    def render(self) -> str:
        head_parts = [self.date.isoformat(), self.flag]
        if self.payee:
            head_parts.append(f'"{q(self.payee)}"')
        head_parts.append(f'"{q(self.narration)}"')
        for t in self.tags:
            head_parts.append(f"#{t}")
        for ln in self.links:
            head_parts.append(f"^{ln}")
        lines = [" ".join(head_parts)]
        for k, v in self.meta:
            lines.append(f"  {k}: {v}")
        for p in self.postings:
            lines.append(p.render())
        return "\n".join(lines)


def render_simplefin_txn(
    *,
    when: date,
    payee: str | None,
    narration: str,
    bank_account: str,
    expense_account: str,
    amount: Decimal,  # signed POV of the bank account (negative = charge)
    sf_id: str,
    project: str | None = None,
    note: str | None = None,
    tags: list[str] | None = None,
) -> Txn:
    txn_id = mint_uuid()
    bank_post = Posting(
        account=bank_account,
        amount=amount,
        meta=[
            ('lamella-source-0', '"simplefin"'),
            ('lamella-source-reference-id-0', f'"{q(sf_id)}"'),
        ],
    )
    expense_post = Posting(account=expense_account, amount=-amount)
    meta: list[tuple[str, str]] = [('lamella-txn-id', f'"{txn_id}"')]
    if project:
        meta.append(('project', f'"{q(project)}"'))
    if note:
        meta.append(('note', f'"{q(note)}"'))
    return Txn(
        date=when,
        payee=payee,
        narration=narration,
        postings=[bank_post, expense_post],
        tags=tags or [],
        meta=meta,
    )


# ---------------------------------------------------------------
# Mortgage amortization
# ---------------------------------------------------------------

def amortization_schedule(
    *,
    principal: Decimal,
    annual_rate: Decimal,
    years: int,
    start: date,
) -> list[tuple[date, Decimal, Decimal, Decimal]]:
    """Return list of (date, interest, principal, balance) per month.
    The schedule starts at the FIRST PAYMENT date (one month after `start`)
    and runs for years*12 months.
    """
    months = years * 12
    r = annual_rate / Decimal("12")
    if r == 0:
        pay = principal / months
    else:
        # M = P * r * (1+r)^n / ((1+r)^n - 1)
        one_plus_r_n = (Decimal("1") + r) ** months
        pay = principal * r * one_plus_r_n / (one_plus_r_n - Decimal("1"))
    pay = pay.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    rows: list[tuple[date, Decimal, Decimal, Decimal]] = []
    bal = principal
    cur = start
    for _ in range(months):
        # Move to next month, same day-of-month if possible
        y = cur.year + (1 if cur.month == 12 else 0)
        m = 1 if cur.month == 12 else cur.month + 1
        d = min(cur.day, calendar.monthrange(y, m)[1])
        cur = date(y, m, d)
        interest = (bal * r).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        principal_paid = pay - interest
        bal = bal - principal_paid
        rows.append((cur, interest, principal_paid, bal))
    return rows


# ---------------------------------------------------------------
# Accounts.bean
# ---------------------------------------------------------------

ACCOUNTS_OPEN_DATE_PERSONAL_BANK = date(2008, 5, 1)
ACCOUNTS_OPEN_DATE_HOUSE = date(2010, 6, 15)
ACCOUNTS_OPEN_DATE_VEHICLE = date(2018, 3, 10)
ACCOUNTS_OPEN_DATE_BUSINESS = date(2019, 2, 14)


def render_accounts_bean(*, header: str) -> str:
    lines = [header.rstrip(), ""]
    # Personal bank + cards
    pb = ACCOUNTS_OPEN_DATE_PERSONAL_BANK.isoformat()
    lines.append(f"{pb} open {PERS_CHECKING} USD")
    lines.append(f"{pb} open {PERS_SAVINGS} USD")
    lines.append(f"{pb} open {PERS_CARD} USD")
    lines.append(f"{pb} open {PERS_INCOME_SALARY} USD")
    lines.append(f"{pb} open {PERS_INCOME_INTEREST} USD")
    lines.append(f"{pb} open {PERS_OPENING} USD")
    lines.append(f"{pb} open {PERS_GROCERIES} USD")
    lines.append(f"{pb} open {PERS_RESTAURANTS} USD")
    lines.append(f"{pb} open {PERS_HEALTHCARE} USD")
    lines.append(f"{pb} open {PERS_PHONE} USD")
    lines.append(f"{pb} open {PERS_INTERNET} USD")
    lines.append(f"{pb} open {PERS_ENTERTAINMENT} USD")
    lines.append(f"{pb} open {PERS_CHARITY} USD")
    lines.append(f"{pb} open {PERS_HOUSEHOLD} USD")
    lines.append(f"{pb} open {PERS_CLOTHING} USD")
    lines.append(f"{pb} open {PERS_TAX_FED} USD")
    lines.append(f"{pb} open {PERS_TAX_STATE} USD")
    lines.append(f"{pb} open {PERS_TAX_FICA} USD")
    lines.append(f"{pb} open {PERS_BANK_FEES} USD")
    # House
    h = ACCOUNTS_OPEN_DATE_HOUSE.isoformat()
    lines.append("")
    lines.append(f"{h} open {HOUSE_ASSET} USD")
    lines.append(f"{h} open {MORTGAGE_LIABILITY} USD")
    lines.append(f"{h} open {HOUSE_PROPERTY_TAX} USD")
    lines.append(f"{h} open {HOUSE_INSURANCE} USD")
    lines.append(f"{h} open {HOUSE_MAINTENANCE} USD")
    lines.append(f"{h} open {HOUSE_REPAIRS} USD")
    lines.append(f"{h} open {HOUSE_UTILITIES} USD")
    lines.append(f"{h} open {HOUSE_HOA} USD")
    lines.append(f"{h} open {HOUSE_MORT_INTEREST} USD")
    lines.append(f"{h} open {MORTGAGE_ESCROW} USD")
    # Vehicle
    v = ACCOUNTS_OPEN_DATE_VEHICLE.isoformat()
    lines.append("")
    lines.append(f"{v} open {VEH_ASSET} USD")
    lines.append(f"{v} open {VEH_FUEL} USD")
    lines.append(f"{v} open {VEH_OIL} USD")
    lines.append(f"{v} open {VEH_TIRES} USD")
    lines.append(f"{v} open {VEH_INSURANCE} USD")
    lines.append(f"{v} open {VEH_MAINTENANCE} USD")
    lines.append(f"{v} open {VEH_REGISTRATION} USD")
    lines.append(f"{v} open {VEH_PARKING} USD")
    # Acme business
    b = ACCOUNTS_OPEN_DATE_BUSINESS.isoformat()
    lines.append("")
    lines.append(f"{b} open {ACME_CHECKING} USD")
    lines.append(f"{b} open {ACME_CARD} USD")
    lines.append(f"{b} open {ACME_INCOME_CONSULTING} USD")
    lines.append(f"{b} open {ACME_INCOME_PRODUCT} USD")
    lines.append(f"{b} open {ACME_OPENING} USD")
    lines.append(f"{b} open {ACME_ADVERTISING} USD")
    lines.append(f"{b} open {ACME_OFFICE} USD")
    lines.append(f"{b} open {ACME_SUPPLIES} USD")
    lines.append(f"{b} open {ACME_PROFESSIONAL} USD")
    lines.append(f"{b} open {ACME_SOFTWARE} USD")
    lines.append(f"{b} open {ACME_TELEPHONE} USD")
    lines.append(f"{b} open {ACME_INSURANCE} USD")
    lines.append(f"{b} open {ACME_MEALS} USD")
    lines.append(f"{b} open {ACME_TRAVEL} USD")
    lines.append(f"{b} open {ACME_BANK_FEES} USD")
    lines.append(f"{b} open {ACME_VEHICLE_MILEAGE} USD")
    lines.append(f"{b} open {ACME_MILEAGE_DEDUCTION} USD")
    lines.append(f"{b} open Equity:{BUSINESS_ENTITY}:OwnerDraws USD")
    lines.append(f"{pb} open Equity:{PERSONAL_ENTITY}:OwnerCapital USD")
    lines.append("")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------
# Manual transactions: opening balances, paychecks, mortgage
# ---------------------------------------------------------------

PAYCHECK_GROSS = Decimal("1700.00")
PAYCHECK_FED = Decimal("220.00")
PAYCHECK_STATE = Decimal("75.00")
PAYCHECK_FICA = Decimal("130.05")
PAYCHECK_NET = (
    PAYCHECK_GROSS - PAYCHECK_FED - PAYCHECK_STATE - PAYCHECK_FICA
)  # = 1274.95 → bi-weekly take-home for a long-tenured single earner

# Bi-weekly Fridays starting 2025-01-03 → 26 paychecks
def biweekly_fridays(year: int) -> list[date]:
    out = []
    d = date(year, 1, 3)  # 2025-01-03 was a Friday
    while d.year == year:
        out.append(d)
        d += timedelta(days=14)
    return out


def render_manual_transactions(
    *, header: str,
    mortgage_schedule_2025: list[tuple[date, Decimal, Decimal, Decimal]],
    opening_balances: dict[str, Decimal],
    business_miles: Decimal,
) -> str:
    lines = [header.rstrip(), ""]

    # Opening balances at 2024-12-31 — bootstrap into 2025 with realistic
    # carryover. The Equity:OpeningBalances account absorbs the sign.
    on = date(2024, 12, 31)
    for account, amount in opening_balances.items():
        entity = (
            BUSINESS_ENTITY if f":{BUSINESS_ENTITY}:" in account
            else PERSONAL_ENTITY
        )
        opening = ACME_OPENING if entity == BUSINESS_ENTITY else PERS_OPENING
        txn = Txn(
            date=on,
            payee=None,
            narration=f"Opening balance ({account.split(':')[-1]})",
            postings=[
                Posting(account=account, amount=amount),
                Posting(account=opening, amount=-amount),
            ],
            meta=[('lamella-txn-id', f'"{mint_uuid()}"')],
        )
        lines.append("")
        lines.append(txn.render())

    # Paychecks (bi-weekly Fridays in 2025)
    for pay_date in biweekly_fridays(DEMO_YEAR):
        lines.append("")
        txn = Txn(
            date=pay_date,
            payee="Acme Co. Payroll",
            narration="Paycheck — bi-weekly",
            postings=[
                Posting(account=PERS_CHECKING, amount=PAYCHECK_NET),
                Posting(account=PERS_TAX_FED, amount=PAYCHECK_FED),
                Posting(account=PERS_TAX_STATE, amount=PAYCHECK_STATE),
                Posting(account=PERS_TAX_FICA, amount=PAYCHECK_FICA),
                Posting(
                    account=PERS_INCOME_SALARY,
                    amount=-PAYCHECK_GROSS,
                ),
            ],
            meta=[('lamella-txn-id', f'"{mint_uuid()}"')],
        )
        lines.append(txn.render())

    # Mortgage payments — 12 months in 2025. Each is a 4-leg txn:
    #   Checking -1105.00
    #   Mortgage Liability +principal
    #   MortgageInterest +interest
    #   Escrow +escrow
    PROPERTY_ESCROW = Decimal("250.00")  # tax + insurance escrow per month
    for pay_date, interest, principal, _bal in mortgage_schedule_2025:
        total = (interest + principal + PROPERTY_ESCROW).quantize(
            Decimal("0.01")
        )
        lines.append("")
        txn = Txn(
            date=pay_date,
            payee="BankOne Mortgage Servicing",
            narration=(
                f"Mortgage payment (interest {fmt_money(interest)}, "
                f"principal {fmt_money(principal)}, escrow "
                f"{fmt_money(PROPERTY_ESCROW)})"
            ),
            postings=[
                Posting(account=PERS_CHECKING, amount=-total),
                Posting(account=MORTGAGE_LIABILITY, amount=principal),
                Posting(account=HOUSE_MORT_INTEREST, amount=interest),
                Posting(account=MORTGAGE_ESCROW, amount=PROPERTY_ESCROW),
            ],
            meta=[
                ('lamella-txn-id', f'"{mint_uuid()}"'),
                ('lamella-loan-slug', f'"{MORTGAGE_SLUG}"'),
            ],
        )
        lines.append(txn.render())

    # Property tax (one annual payment via direct check, separate from
    # escrow-collected portion — small additional bill)
    lines.append("")
    lines.append(Txn(
        date=date(2025, 11, 15),
        payee="County Treasurer",
        narration="Annual property tax balance",
        postings=[
            Posting(account=PERS_CHECKING, amount=Decimal("-650.00")),
            Posting(account=HOUSE_PROPERTY_TAX, amount=Decimal("650.00")),
        ],
        meta=[
            ('lamella-txn-id', f'"{mint_uuid()}"'),
            ('schedule', '"A-line-5b"'),
            ('note', '"Annual property tax — escrow shortage settle-up."'),
        ],
    ).render())

    # Homeowners insurance annual top-up (escrow short)
    lines.append("")
    lines.append(Txn(
        date=date(2025, 6, 1),
        payee="Acme Insurance Group",
        narration="Homeowners insurance — annual rider",
        postings=[
            Posting(account=PERS_CHECKING, amount=Decimal("-285.00")),
            Posting(account=HOUSE_INSURANCE, amount=Decimal("285.00")),
        ],
        meta=[('lamella-txn-id', f'"{mint_uuid()}"')],
    ).render())

    # Quarterly estimated federal tax payments (covers the year's
    # additional tax liability from Acme self-employment income).
    for d, amt in [
        (date(2025, 4, 15), Decimal("-450.00")),
        (date(2025, 6, 15), Decimal("-450.00")),
        (date(2025, 9, 15), Decimal("-450.00")),
        (date(2026, 1, 15), Decimal("-450.00")),
    ]:
        if d.year != DEMO_YEAR:
            continue
        lines.append("")
        lines.append(Txn(
            date=d,
            payee="US Treasury",
            narration="Quarterly estimated federal income tax",
            postings=[
                Posting(account=PERS_CHECKING, amount=amt),
                Posting(account=PERS_TAX_FED, amount=-amt),
            ],
            meta=[('lamella-txn-id', f'"{mint_uuid()}"'),
                  ('schedule', '"1040-ES"')],
        ).render())

    # Summer vacation — drains savings for a real big-ticket
    # personal experience, brings balance toward the target carry.
    lines.append("")
    lines.append(Txn(
        date=date(2025, 7, 12),
        payee="TripStone Travel",
        narration="Summer family vacation package",
        postings=[
            Posting(account=PERS_SAVINGS, amount=Decimal("-2800.00")),
            Posting(account=PERS_ENTERTAINMENT,
                    amount=Decimal("2800.00")),
        ],
        meta=[('lamella-txn-id', f'"{mint_uuid()}"'),
              ('note', '"Annual vacation; paid from savings."')],
    ).render())

    # Monthly gym membership
    for m in range(1, 13):
        d = date(DEMO_YEAR, m, 7)
        lines.append("")
        lines.append(Txn(
            date=d, payee="Riverbend Fitness",
            narration="Gym membership — monthly",
            postings=[
                Posting(account=PERS_CHECKING,
                        amount=Decimal("-49.00")),
                Posting(account=PERS_HEALTHCARE,
                        amount=Decimal("49.00")),
            ],
            meta=[('lamella-txn-id', f'"{mint_uuid()}"')],
        ).render())

    # Quarterly interest credit on savings
    for d, amt in [
        (date(2025, 3, 31), Decimal("8.42")),
        (date(2025, 6, 30), Decimal("9.18")),
        (date(2025, 9, 30), Decimal("10.05")),
        (date(2025, 12, 31), Decimal("10.94")),
    ]:
        lines.append("")
        lines.append(Txn(
            date=d,
            payee="BankOne",
            narration="Savings interest credit",
            postings=[
                Posting(account=PERS_SAVINGS, amount=amt),
                Posting(account=PERS_INCOME_INTEREST, amount=-amt),
            ],
            meta=[('lamella-txn-id', f'"{mint_uuid()}"')],
        ).render())

    # Credit-card monthly payoff (auto-pay from checking each month).
    # Sized to match what flows ONTO the card (groceries, restaurants,
    # gas, healthcare, clothing, entertainment, home maintenance,
    # vehicle maintenance, phone) so the card balance trends toward
    # carry-in level — i.e., John pays it off rather than carrying.
    cc_payments = {
        1: Decimal("840.00"),
        2: Decimal("765.00"),
        3: Decimal("920.00"),
        4: Decimal("710.00"),
        5: Decimal("885.00"),
        6: Decimal("1010.00"),
        7: Decimal("795.00"),
        8: Decimal("875.00"),
        9: Decimal("960.00"),
        10: Decimal("805.00"),
        11: Decimal("890.00"),
        12: Decimal("950.00"),
    }
    for m, amt in cc_payments.items():
        d = date(DEMO_YEAR, m, 22)
        lines.append("")
        lines.append(Txn(
            date=d,
            payee=None,
            narration=f"Credit card auto-pay ({calendar.month_name[m]})",
            postings=[
                Posting(account=PERS_CHECKING, amount=-amt),
                Posting(account=PERS_CARD, amount=amt),
            ],
            meta=[('lamella-txn-id', f'"{mint_uuid()}"')],
        ).render())

    # Acme business: monthly owner draw to personal savings — small
    # ($300/mo). Self-employment estimated tax (next block) is the
    # bigger Acme outflow. Modeled as a 4-leg cross-entity transfer:
    # Acme equity acknowledges the draw out, Personal equity records
    # the contribution in.
    for m in range(1, 13):
        d = date(DEMO_YEAR, m, 28)
        lines.append("")
        lines.append(Txn(
            date=d,
            payee=None,
            narration=f"Owner draw → personal savings ({calendar.month_name[m]})",
            postings=[
                Posting(account=ACME_CHECKING, amount=Decimal("-300.00")),
                Posting(
                    account=f"Equity:{BUSINESS_ENTITY}:OwnerDraws",
                    amount=Decimal("300.00"),
                ),
                Posting(
                    account=PERS_SAVINGS,
                    amount=Decimal("300.00"),
                ),
                Posting(
                    account=f"Equity:{PERSONAL_ENTITY}:OwnerCapital",
                    amount=Decimal("-300.00"),
                ),
            ],
            meta=[('lamella-txn-id', f'"{mint_uuid()}"')],
        ).render())

    # Acme: quarterly self-employment estimated tax (federal portion).
    # Drains business checking by the larger share most years. Routes
    # via Equity:Acme:OwnerDraws because SE tax is a personal-side
    # liability — the business pays it on behalf of the owner.
    for d in [date(2025, 4, 15), date(2025, 6, 15),
              date(2025, 9, 15), date(2026, 1, 15)]:
        if d.year != DEMO_YEAR:
            continue
        lines.append("")
        lines.append(Txn(
            date=d, payee="US Treasury",
            narration="Quarterly self-employment estimated tax",
            postings=[
                Posting(account=ACME_CHECKING,
                        amount=Decimal("-3500.00")),
                Posting(account=f"Equity:{BUSINESS_ENTITY}:OwnerDraws",
                        amount=Decimal("3500.00")),
                Posting(account=PERS_TAX_FED,
                        amount=Decimal("3500.00")),
                Posting(account=f"Equity:{PERSONAL_ENTITY}:OwnerCapital",
                        amount=Decimal("-3500.00")),
            ],
            meta=[('lamella-txn-id', f'"{mint_uuid()}"'),
                  ('schedule', '"1040-ES — SE tax portion"')],
        ).render())

    # Year-end mileage deduction journal — Schedule C standard mileage
    # The mileage_summary.bean writeback would normally do this; we
    # render the equivalent entry here so the demo includes the
    # business-mileage Schedule C effect without needing the writer.
    # business_miles is computed from mileage/vehicles.csv totals.
    deduction = (business_miles * MILEAGE_RATE).quantize(Decimal("0.01"))
    lines.append("")
    lines.append(Txn(
        date=date(2025, 12, 31),
        payee=None,
        narration=(
            f"Year-end mileage deduction — "
            f"{business_miles} business mi × ${MILEAGE_RATE}/mi"
        ),
        tags=["lamella-mileage"],
        postings=[
            Posting(account=ACME_VEHICLE_MILEAGE, amount=deduction),
            Posting(account=ACME_MILEAGE_DEDUCTION, amount=-deduction),
        ],
        meta=[
            ('lamella-txn-id', f'"{mint_uuid()}"'),
            ('lamella-mileage-vehicle', f'"{VEHICLE_SLUG}"'),
            ('lamella-mileage-entity', f'"{BUSINESS_ENTITY}"'),
            ('lamella-mileage-miles', fmt_money(business_miles)),
            ('lamella-mileage-rate', fmt_money(MILEAGE_RATE)),
        ],
    ).render())

    # Open the OwnerDraws / OwnerCapital accounts (lazy auto_accounts will
    # also handle this, but we emit explicit Open directives via accounts
    # bean for clarity — see render_accounts_bean wrapping below.)
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------
# SimpleFIN-style daily-life txns
# ---------------------------------------------------------------

# Synthetic merchant catalogs — placeholders only
PERS_MERCHANTS_GROCERIES = [
    "Greenleaf Market", "Cornerstone Grocers", "Sunrise Foods",
    "Harvest Lane Co-op",
]
PERS_MERCHANTS_RESTAURANTS = [
    "Bluebird Diner", "Tortuga Taqueria", "Old Mill Pub",
    "Sakura Ramen House", "Riverside Cafe",
]
PERS_MERCHANTS_GAS = ["Cardinal Fuel", "Oakridge Gas Co.", "Highway 12 Stop"]
PERS_MERCHANTS_HOUSEHOLD = [
    "MainStreet Hardware", "Lakeview Home Center", "Bluefield Garden Supply",
]
PERS_MERCHANTS_CLOTHING = [
    "Walden Apparel", "Northwind Outfitters", "Chestnut & Co.",
]
PERS_MERCHANTS_ENTERTAINMENT = [
    "Bayside Cinemas", "Riverbend Streaming", "Cascade Books",
]
PERS_MERCHANTS_HEALTHCARE = [
    "Plainview Family Practice", "Cedar Pharmacy", "Northshore Dental",
]
PERS_MERCHANTS_INSURANCE = ["Acme Insurance Group", "Coastal Mutual Auto"]
PERS_MERCHANTS_PHONE = ["Telemark Wireless"]
PERS_MERCHANTS_INTERNET = ["FiberOne ISP"]

ACME_MERCHANTS_SUPPLIES = [
    "PaperWorks Wholesale", "Citywide Office Supply", "Industrial Direct",
]
ACME_MERCHANTS_SOFTWARE = [
    "Cloudvault SaaS", "Pipeline Project Tools", "DocuStream",
    "Bytefield Hosting",
]
ACME_MERCHANTS_ADVERTISING = ["Northwind Ads", "GreyMatter Marketing"]
ACME_MERCHANTS_PROF = ["Watershed CPA Group", "Bramble & Stone LLP"]
ACME_MERCHANTS_MEALS = [
    "Bluebird Diner", "Old Mill Pub", "Riverside Cafe",
    "Aurora Bistro",
]
ACME_MERCHANTS_TRAVEL = [
    "TripStone Travel", "Skyline Lodging Inc.", "Coastal Air",
]
ACME_MERCHANTS_OFFICE = [
    "GreyDesk Furniture", "PrintLab Print Shop",
]


def random_dates_in_year(n: int, year: int = DEMO_YEAR) -> list[date]:
    """n random unique dates in `year`, sorted."""
    days = (date(year, 12, 31) - date(year, 1, 1)).days + 1
    used = set()
    while len(used) < n:
        used.add(date(year, 1, 1) + timedelta(days=random.randrange(days)))
    return sorted(used)


def render_simplefin_transactions(*, header: str) -> tuple[str, list[str]]:
    """Returns (file_text, list_of_txn_hashes_for_links)."""
    lines = [header.rstrip(), ""]
    sf_idx = [0]

    def next_sfid(prefix: str) -> str:
        sf_idx[0] += 1
        return f"{prefix}-{sf_idx[0]:05d}"

    def add(txn: Txn) -> None:
        lines.append("")
        lines.append(txn.render())

    # Personal: groceries, ~weekly
    for d in random_dates_in_year(48):
        merchant = random.choice(PERS_MERCHANTS_GROCERIES)
        amt = -Decimal(f"{random.uniform(58, 165):.2f}")
        add(render_simplefin_txn(
            when=d, payee=merchant, narration="Weekly groceries",
            bank_account=PERS_CARD, expense_account=PERS_GROCERIES,
            amount=amt, sf_id=next_sfid("pers"),
        ))

    # Personal: restaurants ~3x/mo
    for d in random_dates_in_year(36):
        merchant = random.choice(PERS_MERCHANTS_RESTAURANTS)
        amt = -Decimal(f"{random.uniform(14, 78):.2f}")
        add(render_simplefin_txn(
            when=d, payee=merchant, narration="Dinner out",
            bank_account=PERS_CARD, expense_account=PERS_RESTAURANTS,
            amount=amt, sf_id=next_sfid("pers"),
        ))

    # Personal: gas ~2x/mo
    for d in random_dates_in_year(24):
        merchant = random.choice(PERS_MERCHANTS_GAS)
        amt = -Decimal(f"{random.uniform(28, 68):.2f}")
        add(render_simplefin_txn(
            when=d, payee=merchant, narration="Fuel — personal vehicle",
            bank_account=PERS_CARD, expense_account=VEH_FUEL,
            amount=amt, sf_id=next_sfid("pers"),
        ))

    # Personal: utilities — 12 monthly
    for m in range(1, 13):
        d = date(DEMO_YEAR, m, 12)
        # Electric — winter higher
        elec = Decimal(f"{random.uniform(95, 165):.2f}")
        if m in (12, 1, 2):
            elec += Decimal("40")
        elec = -elec.quantize(Decimal("0.01"))
        add(render_simplefin_txn(
            when=d, payee="Greenline Power",
            narration="Electric utility",
            bank_account=PERS_CHECKING, expense_account=HOUSE_UTILITIES,
            amount=elec, sf_id=next_sfid("pers"),
            note="Greenline Power — auto-pay residential service.",
        ))
        # Gas
        gas = Decimal(f"{random.uniform(38, 95):.2f}")
        if m in (12, 1, 2, 3):
            gas += Decimal("35")
        gas = -gas.quantize(Decimal("0.01"))
        add(render_simplefin_txn(
            when=d + timedelta(days=2), payee="Northland Natural Gas",
            narration="Gas utility",
            bank_account=PERS_CHECKING, expense_account=HOUSE_UTILITIES,
            amount=gas, sf_id=next_sfid("pers"),
        ))
        # Water
        water = -Decimal(f"{random.uniform(35, 65):.2f}")
        add(render_simplefin_txn(
            when=d + timedelta(days=4), payee="Ridgepoint Water Dist.",
            narration="Water + sewer",
            bank_account=PERS_CHECKING, expense_account=HOUSE_UTILITIES,
            amount=water, sf_id=next_sfid("pers"),
        ))
        # Internet (also home-office portion of business — but kept personal
        # in the demo for simplicity; the real flow would split it).
        add(render_simplefin_txn(
            when=d + timedelta(days=6), payee="FiberOne ISP",
            narration="Home internet",
            bank_account=PERS_CHECKING, expense_account=PERS_INTERNET,
            amount=Decimal("-79.95"), sf_id=next_sfid("pers"),
        ))
        # Phone
        add(render_simplefin_txn(
            when=d + timedelta(days=8), payee="Telemark Wireless",
            narration="Cell phone — personal line",
            bank_account=PERS_CARD, expense_account=PERS_PHONE,
            amount=Decimal("-58.00"), sf_id=next_sfid("pers"),
        ))

    # Personal: vehicle insurance — quarterly
    for d in [date(2025, 1, 8), date(2025, 4, 8), date(2025, 7, 8),
              date(2025, 10, 8)]:
        add(render_simplefin_txn(
            when=d, payee="Coastal Mutual Auto",
            narration="Auto insurance — quarterly",
            bank_account=PERS_CHECKING, expense_account=VEH_INSURANCE,
            amount=Decimal("-318.00"), sf_id=next_sfid("pers"),
        ))

    # Personal: vehicle maintenance / oil — twice
    for d, kind, amt in [
        (date(2025, 4, 22), "Oil change", Decimal("-58.49")),
        (date(2025, 10, 11), "Tire rotation + alignment",
         Decimal("-145.00")),
    ]:
        add(render_simplefin_txn(
            when=d, payee="MainStreet Auto Service",
            narration=kind,
            bank_account=PERS_CARD,
            expense_account=(
                VEH_OIL if "Oil" in kind else VEH_MAINTENANCE
            ),
            amount=amt, sf_id=next_sfid("pers"),
            note=f"Routine maintenance; vehicle slug {VEHICLE_SLUG}.",
        ))

    # Personal: vehicle registration — annual
    add(render_simplefin_txn(
        when=date(2025, 3, 11),
        payee="State Department of Motor Vehicles",
        narration="Annual vehicle registration renewal",
        bank_account=PERS_CHECKING, expense_account=VEH_REGISTRATION,
        amount=Decimal("-94.00"), sf_id=next_sfid("pers"),
    ))

    # Personal: HOA monthly $45
    for m in range(1, 13):
        d = date(DEMO_YEAR, m, 3)
        add(render_simplefin_txn(
            when=d, payee="Maple Hollow HOA",
            narration="HOA dues",
            bank_account=PERS_CHECKING, expense_account=HOUSE_HOA,
            amount=Decimal("-45.00"), sf_id=next_sfid("pers"),
        ))

    # Personal: home maintenance / repairs — sprinkled
    home_maint = [
        (date(2025, 4, 14), "Spring lawn fertilizer",
         Decimal("-62.25"), HOUSE_MAINTENANCE),
        (date(2025, 5, 22), "HVAC service call",
         Decimal("-185.00"), HOUSE_MAINTENANCE),
        (date(2025, 7, 9), "Replacement faucet kitchen",
         Decimal("-138.99"), HOUSE_REPAIRS),
        (date(2025, 9, 30), "Gutter cleaning",
         Decimal("-95.00"), HOUSE_MAINTENANCE),
        (date(2025, 11, 4), "Leaking pipe — plumber visit",
         Decimal("-275.00"), HOUSE_REPAIRS),
    ]
    for d, label, amt, acct in home_maint:
        add(render_simplefin_txn(
            when=d, payee="Maple Hollow Hardware" if "lawn" in label
                else "Greenview Home Services",
            narration=label, bank_account=PERS_CARD,
            expense_account=acct, amount=amt,
            sf_id=next_sfid("pers"),
        ))

    # Personal: healthcare
    for d, label, amt in [
        (date(2025, 2, 18), "Annual physical co-pay", Decimal("-45.00")),
        (date(2025, 5, 30), "Pharmacy refill", Decimal("-32.50")),
        (date(2025, 9, 14), "Dental cleaning co-pay", Decimal("-65.00")),
        (date(2025, 11, 22), "Pharmacy refill", Decimal("-28.95")),
    ]:
        add(render_simplefin_txn(
            when=d,
            payee=random.choice(PERS_MERCHANTS_HEALTHCARE),
            narration=label, bank_account=PERS_CARD,
            expense_account=PERS_HEALTHCARE, amount=amt,
            sf_id=next_sfid("pers"),
        ))

    # Personal: clothing seasonal
    for d, amt in [
        (date(2025, 3, 22), Decimal("-128.00")),
        (date(2025, 8, 15), Decimal("-94.50")),
        (date(2025, 11, 28), Decimal("-215.00")),  # Black-Friday-esque
    ]:
        add(render_simplefin_txn(
            when=d, payee=random.choice(PERS_MERCHANTS_CLOTHING),
            narration="Seasonal clothing", bank_account=PERS_CARD,
            expense_account=PERS_CLOTHING, amount=amt,
            sf_id=next_sfid("pers"),
        ))

    # Personal: entertainment
    for _ in range(14):
        d = random_dates_in_year(1)[0]
        amt = -Decimal(f"{random.uniform(8, 38):.2f}")
        add(render_simplefin_txn(
            when=d,
            payee=random.choice(PERS_MERCHANTS_ENTERTAINMENT),
            narration="Movies / streaming / books",
            bank_account=PERS_CARD,
            expense_account=PERS_ENTERTAINMENT, amount=amt,
            sf_id=next_sfid("pers"),
        ))

    # Personal: charity (Schedule A)
    for d, org, amt in [
        (date(2025, 4, 15), "Riverside Public Library Foundation",
         Decimal("-100.00")),
        (date(2025, 6, 20), "Maple Hollow Food Bank",
         Decimal("-50.00")),
        (date(2025, 12, 18), "Riverside Public Library Foundation",
         Decimal("-200.00")),
        (date(2025, 12, 22), "Children of the Forest Conservancy",
         Decimal("-150.00")),
    ]:
        add(render_simplefin_txn(
            when=d, payee=org, narration="Charitable donation",
            bank_account=PERS_CHECKING,
            expense_account=PERS_CHARITY, amount=amt,
            sf_id=next_sfid("pers"),
            note="Schedule A line 11 — charitable contributions.",
        ))

    # ----- Acme business txns -----

    # Consulting income: ~monthly invoices, varying
    consulting_income = [
        (date(2025, 1, 15), "Brightstar Co.", Decimal("6500.00"),
         "January retainer", PROJECT_NAME),
        (date(2025, 2, 14), "Northwind Holdings", Decimal("4200.00"),
         "February engagement"),
        (date(2025, 3, 28), "Brightstar Co.", Decimal("8800.00"),
         "Phase II milestone", PROJECT_NAME),
        (date(2025, 4, 30), "Cobalt Industries", Decimal("3400.00"),
         "April advisory"),
        (date(2025, 5, 15), "Brightstar Co.", Decimal("6500.00"),
         "May retainer", PROJECT_NAME),
        (date(2025, 6, 17), "Northwind Holdings", Decimal("5100.00"),
         "Quarterly retainer"),
        (date(2025, 7, 18), "Brightstar Co.", Decimal("7250.00"),
         "Phase III deliverables", PROJECT_NAME),
        (date(2025, 8, 12), "Cobalt Industries", Decimal("2900.00"),
         "August project"),
        (date(2025, 9, 10), "Brightstar Co.", Decimal("6500.00"),
         "September retainer", PROJECT_NAME),
        (date(2025, 10, 22), "Cobalt Industries", Decimal("4500.00"),
         "October consulting"),
        (date(2025, 11, 14), "Northwind Holdings", Decimal("3800.00"),
         "Year-end planning"),
        (date(2025, 12, 18), "Brightstar Co.", Decimal("9200.00"),
         "Phase IV final", PROJECT_NAME),
    ]
    for row in consulting_income:
        d, payer, amt = row[0], row[1], row[2]
        narration = row[3]
        proj = row[4] if len(row) > 4 else None
        sfid = next_sfid("acme-inc")
        meta: list[tuple[str, str]] = [
            ('lamella-txn-id', f'"{mint_uuid()}"'),
        ]
        if proj:
            meta.append(('project', f'"{proj}"'))
            meta.append(('note', f'"{proj} milestone payment from {payer}."'))
        else:
            meta.append(('note', f'"Engagement: {narration} — {payer}."'))
        add(Txn(
            date=d, payee=payer, narration=narration,
            postings=[
                Posting(
                    account=ACME_CHECKING, amount=amt,
                    meta=[
                        ('lamella-source-0', '"simplefin"'),
                        ('lamella-source-reference-id-0', f'"{sfid}"'),
                    ],
                ),
                Posting(
                    account=ACME_INCOME_CONSULTING, amount=-amt,
                ),
            ],
            meta=meta,
        ))

    # Acme: software subscriptions monthly
    saas_monthly = [
        ("Cloudvault SaaS", Decimal("-49.00")),
        ("Pipeline Project Tools", Decimal("-29.00")),
        ("DocuStream", Decimal("-19.00")),
        ("Bytefield Hosting", Decimal("-22.50")),
    ]
    for m in range(1, 13):
        for name, amt in saas_monthly:
            d = date(DEMO_YEAR, m, 6 + saas_monthly.index((name, amt)))
            add(render_simplefin_txn(
                when=d, payee=name,
                narration=f"{name} — monthly subscription",
                bank_account=ACME_CARD,
                expense_account=ACME_SOFTWARE,
                amount=amt, sf_id=next_sfid("acme"),
            ))

    # Acme: advertising — about 9 charges
    for _ in range(9):
        d = random_dates_in_year(1)[0]
        amt = -Decimal(f"{random.uniform(85, 320):.2f}")
        add(render_simplefin_txn(
            when=d,
            payee=random.choice(ACME_MERCHANTS_ADVERTISING),
            narration="Online advertising spend",
            bank_account=ACME_CARD,
            expense_account=ACME_ADVERTISING, amount=amt,
            sf_id=next_sfid("acme"),
        ))

    # Acme: supplies — ~10 charges
    for _ in range(11):
        d = random_dates_in_year(1)[0]
        amt = -Decimal(f"{random.uniform(18, 145):.2f}")
        add(render_simplefin_txn(
            when=d,
            payee=random.choice(ACME_MERCHANTS_SUPPLIES),
            narration="Office supplies",
            bank_account=ACME_CARD,
            expense_account=ACME_SUPPLIES, amount=amt,
            sf_id=next_sfid("acme"),
            project=(
                PROJECT_NAME if random.random() < 0.3 else None
            ),
        ))

    # Acme: client meals (Sched C 24b)
    for _ in range(8):
        d = random_dates_in_year(1)[0]
        amt = -Decimal(f"{random.uniform(28, 95):.2f}")
        add(render_simplefin_txn(
            when=d,
            payee=random.choice(ACME_MERCHANTS_MEALS),
            narration="Client meeting — meals",
            bank_account=ACME_CARD,
            expense_account=ACME_MEALS, amount=amt,
            sf_id=next_sfid("acme"),
            note="50%-deductible client meal per IRS Pub 463.",
        ))

    # Acme: travel — quarterly trips
    travel_trips = [
        (date(2025, 2, 18), "Coastal Air", "Client visit airfare",
         Decimal("-385.00")),
        (date(2025, 2, 19), "Skyline Lodging Inc.",
         "Client visit lodging — 2 nights", Decimal("-298.00")),
        (date(2025, 5, 14), "TripStone Travel",
         "Conference registration + travel package", Decimal("-625.00")),
        (date(2025, 8, 20), "Coastal Air",
         "Vendor meeting airfare", Decimal("-410.00")),
        (date(2025, 8, 21), "Skyline Lodging Inc.",
         "Vendor meeting hotel", Decimal("-185.00")),
        (date(2025, 11, 6), "TripStone Travel",
         "Year-end client offsite", Decimal("-545.00")),
    ]
    for d, payer, narr, amt in travel_trips:
        add(render_simplefin_txn(
            when=d, payee=payer, narration=narr,
            bank_account=ACME_CARD, expense_account=ACME_TRAVEL,
            amount=amt, sf_id=next_sfid("acme"),
            note=f"Business trip; see mileage log entry on or near {d.isoformat()}.",
        ))

    # Acme: professional fees (CPA quarterly)
    for d in [date(2025, 3, 31), date(2025, 6, 30),
              date(2025, 9, 30), date(2025, 12, 31)]:
        add(render_simplefin_txn(
            when=d, payee="Watershed CPA Group",
            narration="Quarterly bookkeeping retainer",
            bank_account=ACME_CHECKING,
            expense_account=ACME_PROFESSIONAL,
            amount=Decimal("-450.00"), sf_id=next_sfid("acme"),
        ))

    # Acme: legal — once
    add(render_simplefin_txn(
        when=date(2025, 7, 22), payee="Bramble & Stone LLP",
        narration="Contract review",
        bank_account=ACME_CHECKING,
        expense_account=ACME_PROFESSIONAL,
        amount=Decimal("-385.00"), sf_id=next_sfid("acme"),
        project=PROJECT_NAME,
        note=f"{PROJECT_NAME} — MSA contract review.",
    ))

    # Acme: phone (separate business line)
    for m in range(1, 13):
        d = date(DEMO_YEAR, m, 18)
        add(render_simplefin_txn(
            when=d, payee="Telemark Wireless",
            narration="Business cellular line",
            bank_account=ACME_CARD,
            expense_account=ACME_TELEPHONE,
            amount=Decimal("-65.00"), sf_id=next_sfid("acme"),
        ))

    # Acme: business insurance — annual
    add(render_simplefin_txn(
        when=date(2025, 4, 5), payee="Acme Insurance Group",
        narration="General-liability + E&O insurance — annual",
        bank_account=ACME_CHECKING,
        expense_account=ACME_INSURANCE,
        amount=Decimal("-1185.00"), sf_id=next_sfid("acme"),
    ))

    # Acme: bank fees monthly
    for m in range(1, 13):
        d = date(DEMO_YEAR, m, 1)
        add(render_simplefin_txn(
            when=d, payee=BANK_INST,
            narration="Business checking — monthly maintenance fee",
            bank_account=ACME_CHECKING,
            expense_account=ACME_BANK_FEES,
            amount=Decimal("-12.00"), sf_id=next_sfid("acme"),
        ))

    # Acme: card payoff (auto-pay) monthly from Acme checking
    cc_acme_payments = {
        1: Decimal("520.00"), 2: Decimal("612.00"),
        3: Decimal("488.00"), 4: Decimal("1245.00"),
        5: Decimal("675.00"), 6: Decimal("840.00"),
        7: Decimal("520.00"), 8: Decimal("930.00"),
        9: Decimal("710.00"), 10: Decimal("615.00"),
        11: Decimal("805.00"), 12: Decimal("780.00"),
    }
    for m, amt in cc_acme_payments.items():
        d = date(DEMO_YEAR, m, 24)
        add(render_simplefin_txn(
            when=d, payee=None,
            narration=f"Acme card auto-pay ({calendar.month_name[m]})",
            bank_account=ACME_CHECKING,
            expense_account=ACME_CARD, amount=-amt,
            sf_id=next_sfid("acme"),
        ))
        # Note: the "expense_account" is actually the card liability
        # being paid down; render_simplefin_txn uses it as the
        # offsetting leg, which is what we want.

    # Acme: office furniture / one-off office expenses
    add(render_simplefin_txn(
        when=date(2025, 2, 7), payee="GreyDesk Furniture",
        narration="Standing desk + chair",
        bank_account=ACME_CARD,
        expense_account=ACME_OFFICE,
        amount=Decimal("-685.00"), sf_id=next_sfid("acme"),
        note="Home-office equipment purchase.",
    ))
    add(render_simplefin_txn(
        when=date(2025, 9, 18), payee="PrintLab Print Shop",
        narration="Business cards + brochures",
        bank_account=ACME_CARD,
        expense_account=ACME_OFFICE,
        amount=Decimal("-148.00"), sf_id=next_sfid("acme"),
        project=PROJECT_NAME,
    ))

    return "\n".join(lines) + "\n", []


# ---------------------------------------------------------------
# Mileage CSV + summary
# ---------------------------------------------------------------

def render_mileage_csv() -> tuple[str, Decimal, Decimal]:
    """Generate the mileage CSV. Returns (csv_text, business_miles,
    personal_miles)."""
    rows = []
    odo = 48000  # starting odometer Jan 1
    business_miles = Decimal("0")
    personal_miles = Decimal("0")

    business_purposes = [
        ("Brightstar Co.", "Office", "Client site"),
        ("Cobalt Industries", "Office", "Vendor meeting"),
        ("Brightstar Co.", "Office", "Project standup"),
        ("Northwind Holdings", "Office", "Quarterly review"),
        ("Office", "Co-working space", "Working session"),
        ("Office", "Conference center", "Industry meetup"),
    ]
    personal_purposes = [
        ("Home", "Greenleaf Market", "Groceries"),
        ("Home", "Plainview Family Practice", "Doctor"),
        ("Home", "Maple Hollow Hardware", "Hardware run"),
        ("Home", "Bayside Cinemas", "Movies"),
        ("Home", "Northshore Dental", "Dentist"),
    ]

    # ~3 business + ~5 personal per month, varied
    cur_date = date(DEMO_YEAR, 1, 1)
    while cur_date.year == DEMO_YEAR:
        # Business 3 entries this month
        for _ in range(random.randint(2, 4)):
            day = random.randint(1, calendar.monthrange(
                cur_date.year, cur_date.month)[1])
            d = date(cur_date.year, cur_date.month, day)
            miles = Decimal(f"{random.uniform(8, 95):.1f}")
            client, frm, to = random.choice(business_purposes)
            note = ""
            if random.random() < 0.3:
                note = f"{PROJECT_NAME} site visit"
            rows.append((
                d.isoformat(), VEHICLE_SLUG, str(odo),
                str(odo + int(miles)), str(miles),
                f"Client visit — {client}", BUSINESS_ENTITY, frm, to, note,
            ))
            odo += int(miles)
            business_miles += miles
        # Personal 5 entries
        for _ in range(random.randint(4, 6)):
            day = random.randint(1, calendar.monthrange(
                cur_date.year, cur_date.month)[1])
            d = date(cur_date.year, cur_date.month, day)
            miles = Decimal(f"{random.uniform(2, 28):.1f}")
            _, frm, to = random.choice(personal_purposes)
            rows.append((
                d.isoformat(), VEHICLE_SLUG, str(odo),
                str(odo + int(miles)), str(miles),
                "Personal trip", PERSONAL_ENTITY, frm, to, "",
            ))
            odo += int(miles)
            personal_miles += miles

        # Move to first of next month
        if cur_date.month == 12:
            break
        cur_date = date(cur_date.year, cur_date.month + 1, 1)

    rows.sort(key=lambda r: r[0])

    out = ["date,vehicle,odometer_start,odometer_end,miles,purpose,entity,from,to,notes"]
    for r in rows:
        out.append(",".join(r))
    return "\n".join(out) + "\n", business_miles, personal_miles


def render_mileage_summary(
    *, header: str, business_miles: Decimal, business_amount: Decimal,
) -> str:
    lines = [header.rstrip(), ""]
    lines.append(f'2025-12-31 custom "lamella-mileage-rate" "{MILEAGE_RATE}" '
                 f'; IRS standard mileage rate (placeholder)')
    lines.append("")
    # Summary entry — non-monetary marker that mileage was reckoned.
    # The actual deduction journal is in manual_transactions.bean.
    lines.append(
        f'2025-12-31 custom "lamella-mileage-summary" "{VEHICLE_SLUG}"'
    )
    lines.append(f'  lamella-mileage-vehicle: "{VEHICLE_SLUG}"')
    lines.append(f'  lamella-mileage-entity: "{BUSINESS_ENTITY}"')
    lines.append(f'  lamella-mileage-miles: {business_miles}')
    lines.append(f'  lamella-mileage-rate: {MILEAGE_RATE}')
    lines.append(f'  lamella-mileage-amount: {business_amount} USD')
    lines.append("")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------
# Connector files (mostly empty-with-header; rules has examples)
# ---------------------------------------------------------------

def render_connector_rules(*, header: str) -> str:
    lines = [header.rstrip(), ""]
    # A few classification rules a real user would have learned.
    examples = [
        ("Greenline Power", HOUSE_UTILITIES, "rule-utilities-electric"),
        ("Northland Natural Gas", HOUSE_UTILITIES, "rule-utilities-gas"),
        ("FiberOne ISP", PERS_INTERNET, "rule-internet"),
        ("Telemark Wireless", PERS_PHONE, "rule-cell"),
        ("Coastal Mutual Auto", VEH_INSURANCE, "rule-vehicle-ins"),
        ("Maple Hollow HOA", HOUSE_HOA, "rule-hoa"),
        ("Cloudvault SaaS", ACME_SOFTWARE, "rule-acme-saas-cv"),
        ("Pipeline Project Tools", ACME_SOFTWARE, "rule-acme-saas-pp"),
        ("DocuStream", ACME_SOFTWARE, "rule-acme-saas-ds"),
        ("Bytefield Hosting", ACME_SOFTWARE, "rule-acme-hosting"),
        ("Watershed CPA Group", ACME_PROFESSIONAL, "rule-acme-cpa"),
    ]
    for payee, target, rule_id in examples:
        lines.append("")
        lines.append(
            f'2025-01-01 custom "classification-rule" "{rule_id}"'
        )
        lines.append('  pattern-type: "merchant_contains"')
        lines.append(f'  pattern-value: "{payee}"')
        lines.append(f'  target-account: "{target}"')
        lines.append('  source: "user"')
    lines.append("")
    return "\n".join(lines) + "\n"


def render_connector_budgets(*, header: str) -> str:
    lines = [header.rstrip(), ""]
    # A handful of monthly budgets
    budgets = [
        (PERS_GROCERIES, Decimal("550.00")),
        (PERS_RESTAURANTS, Decimal("180.00")),
        (PERS_ENTERTAINMENT, Decimal("60.00")),
        (HOUSE_UTILITIES, Decimal("280.00")),
        (ACME_ADVERTISING, Decimal("250.00")),
        (ACME_SOFTWARE, Decimal("150.00")),
    ]
    for account, amount in budgets:
        lines.append("")
        lines.append(
            f'2025-01-01 custom "budget" "{account}"'
        )
        lines.append('  cadence: "monthly"')
        lines.append(f'  target: {amount} USD')
        lines.append('  alert-at: 0.90')
    lines.append("")
    return "\n".join(lines) + "\n"


def render_connector_config(*, header: str) -> str:
    lines = [header.rstrip(), ""]
    lines.append('2025-01-01 custom "setting" "ai-classification-enabled" "TRUE"')
    lines.append('2025-01-01 custom "setting" "auto-match-receipts" "TRUE"')
    lines.append('2025-01-01 custom "setting" "default-currency" "USD"')
    lines.append("")
    # Entity directives — without these, _check_entities flags the
    # install as incomplete and the dashboard gate stays closed even
    # though every other piece of state is reconstructable from the
    # ledger. Match the shape emitted by entity_writer.append_entity_directive.
    lines.append(f'2025-01-01 custom "entity" "{PERSONAL_ENTITY}"')
    lines.append('  lamella-display-name: "John Smith"')
    lines.append('  lamella-entity-type: "personal"')
    lines.append('  lamella-tax-schedule: "A"')
    lines.append('  lamella-start-date: "2010-01-01"')
    lines.append('  lamella-modified-at: "2025-01-01T00:00:00+00:00"')
    lines.append("")
    lines.append(f'2025-01-01 custom "entity" "{BUSINESS_ENTITY}"')
    lines.append('  lamella-display-name: "Acme Co."')
    lines.append('  lamella-entity-type: "sole_proprietorship"')
    lines.append('  lamella-tax-schedule: "C"')
    lines.append('  lamella-start-date: "2018-01-01"')
    lines.append('  lamella-notes: "Single-member consulting + product sales (Schedule C)."')
    lines.append('  lamella-modified-at: "2025-01-01T00:00:00+00:00"')
    lines.append("")
    return "\n".join(lines) + "\n"


def render_connector_accounts(*, header: str) -> str:
    """Account-kind overrides for the demo's bank, card, and loan
    accounts. seed_accounts_meta infers ``checking``/``savings``
    correctly from leaf names but the explicit directives make the
    demo round-trip cleanly through reconstruct (the ledger is the
    source of truth — anything SQLite-only is invisible after a DB
    wipe)."""
    lines = [header.rstrip(), ""]
    overrides: tuple[tuple[str, str], ...] = (
        (PERS_CHECKING, "checking"),
        (PERS_SAVINGS, "savings"),
        (PERS_CARD, "credit_card"),
        (ACME_CHECKING, "checking"),
        (ACME_CARD, "credit_card"),
        (MORTGAGE_LIABILITY, "loan"),
        # House + vehicle assets aren't bank accounts but they're
        # still Assets:* paths that _check_account_labels flags as
        # needing labels. Use kind="asset" to satisfy the check.
        (HOUSE_ASSET, "asset"),
        (VEH_ASSET, "asset"),
    )
    for path, kind in overrides:
        lines.append(f'2025-01-01 custom "account-kind" {path} "{kind}"')
    lines.append("")
    return "\n".join(lines) + "\n"


def render_empty_connector(*, header: str, filename: str) -> str:
    return header + (
        f"\n; {filename} — no Lamella-managed entries yet.\n"
        "; This file is created on first run and stays empty until the\n"
        "; first relevant action (e.g. linking a receipt for "
        "connector_links.bean).\n"
    )


# ---------------------------------------------------------------
# Headers
# ---------------------------------------------------------------

USER_HEADER = """\
;; ---------------------------------------------------------------
;; User-authored file. Edit freely.
;;
;; Lamella reads this file every parse but never rewrites it.
;;
;; File:      {filename}
;; Owner:     user
;; Schema:    lamella-ledger-version=3
;; Generated: {when} by Lamella demo generator
;; ---------------------------------------------------------------
"""

CONNECTOR_HEADER = """\
;; ---------------------------------------------------------------
;; Managed by Lamella. Do not edit by hand.
;;
;; This file is regenerated from user actions in the web UI. Manual
;; edits may be reverted silently on the next write.
;;
;; File:      {filename}
;; Owner:     lamella
;; Schema:    lamella-ledger-version=3
;; Generated: {when} by Lamella demo generator
;; ---------------------------------------------------------------
"""


def main_bean_text(*, when: date) -> str:
    return f"""\
;; ---------------------------------------------------------------
;; Lamella — DEMO LEDGER (public sample)
;;
;; This is a synthetic single-user, single-business ledger covering
;; calendar year {DEMO_YEAR}. All names are placeholders (per
;; ADR-0017): John Smith / Acme Co. / BankOne / fictional merchants.
;;
;; Generated: {when.isoformat()} by scripts/generate_demo.py.
;; Re-run the script to regenerate; it wipes ledger.demo/ first.
;;
;; File:      main.bean
;; Owner:     user
;; Schema:    lamella-ledger-version=3
;; ---------------------------------------------------------------

option "title"              "Lamella Demo Ledger — John Smith / Acme Co."
option "operating_currency" "USD"

2026-01-01 custom "lamella-ledger-version" "3"

plugin "beancount_lazy_plugins.auto_accounts"

include "accounts.bean"
include "commodities.bean"
include "prices.bean"
include "events.bean"
include "manual_transactions.bean"

include "connector_accounts.bean"
include "connector_links.bean"
include "connector_overrides.bean"
include "connector_rules.bean"
include "connector_budgets.bean"
include "connector_config.bean"
include "connector_transfers.bean"
include "simplefin_transactions.bean"

include "mileage_summary.bean"
"""


# ---------------------------------------------------------------
# Driver
# ---------------------------------------------------------------

def write(p: Path, content: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out", default="ledger.demo",
        help="Output directory (default: ledger.demo/)",
    )
    parser.add_argument(
        "--seed", type=int, default=20250101,
        help="RNG seed (default: 20250101)",
    )
    args = parser.parse_args()
    out = Path(args.out)

    random.seed(args.seed)
    when = date.today()

    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    # Compute mortgage schedule for 2025 (months 175-186 of a 360-mo loan
    # that started 2010-06-15 at $150,000 / 5% / 30y).
    full_schedule = amortization_schedule(
        principal=Decimal("150000"),
        annual_rate=Decimal("0.05"),
        years=30,
        start=date(2010, 6, 15),
    )
    schedule_2025 = [r for r in full_schedule if r[0].year == DEMO_YEAR]

    # Opening balances at 2024-12-31 (carry-in to demo year)
    opening = {
        PERS_CHECKING: Decimal("2150.00"),
        PERS_SAVINGS: Decimal("12480.00"),
        PERS_CARD: Decimal("-1340.00"),  # owe $1340
        ACME_CHECKING: Decimal("8625.00"),
        ACME_CARD: Decimal("-2510.00"),  # owe $2510
        # House cost basis (paid 15y ago; remaining liability is mortgage)
        HOUSE_ASSET: Decimal("200000.00"),
        # Vehicle cost basis (paid 7y ago)
        VEH_ASSET: Decimal("18500.00"),
        # Mortgage liability — balance at end of 2024 (Jan 2025 starting bal)
        MORTGAGE_LIABILITY: -Decimal(
            f"{full_schedule[174][3]:.2f}"
        ),  # balance after payment 174 (= end of Dec 2024)
    }

    # Headers
    def uh(name: str) -> str:
        return USER_HEADER.format(filename=name, when=when.isoformat())

    def ch(name: str) -> str:
        return CONNECTOR_HEADER.format(filename=name, when=when.isoformat())

    # Mileage CSV first — manual_transactions needs the total business
    # miles so the year-end deduction journal matches the CSV exactly.
    csv_text, business_miles, personal_miles = render_mileage_csv()
    business_amount = (business_miles * MILEAGE_RATE).quantize(Decimal("0.01"))

    # Files
    write(out / "main.bean", main_bean_text(when=when))
    write(out / "accounts.bean", render_accounts_bean(header=uh("accounts.bean")))
    write(out / "commodities.bean", uh("commodities.bean")
          + "\n; USD-only ledger; no commodity directives.\n")
    write(out / "prices.bean", uh("prices.bean")
          + "\n; USD-only; no price directives.\n")
    write(out / "events.bean", uh("events.bean")
          + "\n2010-06-15 event \"home-purchased\" \"123 Main St, Anytown, ST 00000\"\n"
          + "2018-03-10 event \"vehicle-acquired\" \"2018 sedan; primary household vehicle\"\n"
          + "2025-01-01 event \"tax-year-start\" \"2025 — calendar year\"\n")
    write(
        out / "manual_transactions.bean",
        render_manual_transactions(
            header=uh("manual_transactions.bean"),
            mortgage_schedule_2025=schedule_2025,
            opening_balances=opening,
            business_miles=business_miles,
        ),
    )

    sf_text, _ = render_simplefin_transactions(
        header=ch("simplefin_transactions.bean"),
    )
    write(out / "simplefin_transactions.bean", sf_text)

    write(out / "mileage" / "vehicles.csv", csv_text)
    write(
        out / "mileage_summary.bean",
        render_mileage_summary(
            header=ch("mileage_summary.bean"),
            business_miles=business_miles,
            business_amount=business_amount,
        ),
    )

    # Connector files
    write(
        out / "connector_accounts.bean",
        render_connector_accounts(header=ch("connector_accounts.bean")),
    )
    write(
        out / "connector_links.bean",
        render_empty_connector(
            header=ch("connector_links.bean"),
            filename="connector_links.bean",
        ),
    )
    write(
        out / "connector_overrides.bean",
        render_empty_connector(
            header=ch("connector_overrides.bean"),
            filename="connector_overrides.bean",
        ),
    )
    write(
        out / "connector_rules.bean",
        render_connector_rules(header=ch("connector_rules.bean")),
    )
    write(
        out / "connector_budgets.bean",
        render_connector_budgets(header=ch("connector_budgets.bean")),
    )
    write(
        out / "connector_config.bean",
        render_connector_config(header=ch("connector_config.bean")),
    )
    write(
        out / "connector_transfers.bean",
        render_empty_connector(
            header=ch("connector_transfers.bean"),
            filename="connector_transfers.bean",
        ),
    )

    # README — explains demo content
    readme = f"""\
# Lamella Demo Ledger

Synthetic public sample ledger covering calendar year **{DEMO_YEAR}**
for a single user (John Smith) running a small consulting business
(Acme Co.).

All names are placeholders per ADR-0017. No real merchants, no real
PII. The ledger is reproducibly generated by
`scripts/generate_demo.py` (seed `{args.seed}`).

## Contents

* **Personal accounts** at BankOne: checking (~$2,150 carry-in),
  savings (~$12,480 carry-in), one credit card (~-$1,340 carry-in).
* **Business accounts** for Acme Co. at BankOne: checking
  (~$8,625), one credit card (~-$2,510).
* **Home** ({HOUSE_SLUG}, Anytown ST): purchased 2010-06-15 for
  $200,000. Mortgage of $150,000 at 5% / 30y, 15 years in.
* **Vehicle** ({VEHICLE_SLUG}): acquired 2018-03-10, basis $18,500.
* **Project**: `{PROJECT_NAME}` — a recurring engagement with
  Brightstar Co. that tags consulting income, supplies, and
  professional fees throughout the year.

## What's in the year

* 26 bi-weekly paychecks with federal / state / FICA withholding
  split out (Schedule A relevant items: state tax line 5a).
* 12 monthly mortgage payments; each split as principal / interest /
  escrow. Mortgage interest goes to
  `{HOUSE_MORT_INTEREST}` for Schedule A line 8a.
* Annual property-tax balance, homeowners-insurance rider, monthly
  HOA, utilities (electric / gas / water / internet), phone.
* Vehicle: quarterly insurance, oil change, tire rotation, annual
  registration. Year of mileage logs in `mileage/vehicles.csv`
  (~{int(business_miles)} business mi, ~{int(personal_miles)}
  personal mi → ~{int(100 * business_miles / (business_miles + personal_miles))}%
  business use). Year-end Schedule C standard-mileage deduction
  journaled at 2025-12-31.
* Acme business: monthly SaaS subscriptions, advertising spend,
  client meals, quarterly CPA retainer, an annual general-liability
  insurance renewal, six business trips with linked travel +
  lodging, owner draws to personal savings.
* Acme income: 12 consulting payments from three client entities
  (Brightstar Co., Northwind Holdings, Cobalt Industries).

## Files

```
ledger.demo/
├── main.bean                        # entry point — loads everything
├── accounts.bean                    # Open directives (user-authored)
├── commodities.bean                 # USD-only; empty
├── prices.bean                      # empty
├── events.bean                      # life events (home, vehicle, tax year)
├── manual_transactions.bean         # opening balances, paychecks, mortgage
├── simplefin_transactions.bean      # bank-side classified txns
├── connector_accounts.bean          # (empty in demo; accounts in accounts.bean)
├── connector_links.bean             # (empty in demo)
├── connector_overrides.bean         # (empty in demo)
├── connector_rules.bean             # 11 classification rules
├── connector_budgets.bean           # 6 monthly budgets
├── connector_config.bean            # baseline settings
├── connector_transfers.bean         # (empty in demo)
├── mileage_summary.bean             # year-end summary directive
└── mileage/
    └── vehicles.csv                 # year of mileage log entries
```

## Regenerating

```bash
python scripts/generate_demo.py            # default --out ledger.demo/
python scripts/generate_demo.py --seed 42  # different RNG → different demo
```

The script wipes the output directory before regenerating.

## Verifying

```bash
.venv/bin/bean-check ledger.demo/main.bean
```

Should report zero errors. If it doesn't, the demo generator drifted
from the chart-of-accounts spec; open an issue.
"""
    write(out / "README.md", readme)

    # Summary
    print(f"demo written to {out}/")
    print(f"  business mileage: {business_miles}")
    print(f"  personal mileage: {personal_miles}")
    print(f"  business deduction: ${business_amount}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
