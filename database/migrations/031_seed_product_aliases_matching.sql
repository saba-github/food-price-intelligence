-- Seed aliases for cross-retailer product matching

insert into dim_product_aliases (alias_text, normalized_alias, product_id)
values
    ('Muz İthal', 'muz ithal', 135),
    ('Muz Yerli', 'muz yerli', 139),
    ('Elma Granny Smith', 'elma granny smith', 141),
    ('Elma Starking', 'elma starking', 136),
    ('Biber Carliston', 'biber carliston', 148),
    ('Domates Kokteyl', 'domates kokteyl', 150)
on conflict do nothing;
