-- Seed aliases for cross-retailer product matching

insert into dim_product_aliases (alias_name, product_id)
values
    ('muz i̇thal', 135),
    ('muz yerli', 139),
    ('elma granny smith', 141),
    ('elma starking', 136),
    ('biber carliston', 148),
    ('domates kokteyl', 150)
on conflict do nothing;
