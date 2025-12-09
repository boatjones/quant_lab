INSERT INTO adr_whitelist (ticker, company_name, country, sector, notes)
Values ('DUAVF', 'Dassault Aviation SA', 'France', 'Aerospace & Defense', 'Rafale fighter jets'),
    ('RNMBY', 'Rheinmetall AG', 'Germany', 'Aerospace & Defense', 'Tanks, artillery, ammunition'),
    ('SAABY', 'Saab AB', 'Sweden', 'Aerospace & Defense', 'Gripen fighter jets, naval systems'),
    ('THLLY', 'Thales SA', 'France', 'Aerospace & Defense', 'Electronics, missiles, SAMP/T');
    
INSERT INTO symbols (ticker, company_name, exchange, asset_type, is_etf, is_active, date_loaded)
VALUES ('DUAVF', 'Dassault Aviation SA', 'OTC', 'stock', 0, 1, CURRENT_DATE),
       ('RNMBY', 'Rheinmetall AG', 'OTC', 'stock', 0, 1, CURRENT_DATE),
       ('SAABY', 'Saab AB', 'OTC', 'stock', 0, 1, CURRENT_DATE),
       ('THLLY', 'Thales SA', 'OTC', 'stock', 0, 1, CURRENT_DATE)
ON CONFLICT (ticker) DO NOTHING;

INSERT INTO stocks (ticker, company_name, exchange, industry, sector)
VALUES ('DUAVF', 'Dassault Aviation SA', 'OTC', 'Aerospace & Defense', 'Aerospace & Defense'),
       ('RNMBY', 'Rheinmetall AG', 'OTC', 'Aerospace & Defense', 'Aerospace & Defense'),
       ('SAABY', 'Saab AB', 'OTC', 'Aerospace & Defense', 'Aerospace & Defense'),
       ('THLLY', 'Thales SA', 'OTC', 'Aerospace & Defense', 'Aerospace & Defense')
ON CONFLICT (ticker) DO NOTHING;