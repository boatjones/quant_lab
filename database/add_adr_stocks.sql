-- First pass of adding ADRs
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


-- Second pass of adding ADRs
INSERT INTO adr_whitelist (ticker, company_name, country, sector, notes)
VALUES 
    ('BAESY', 'BAE Systems plc', 'United Kingdom', 'Aerospace & Defense', 'Defense systems, submarines, Eurofighter Typhoon'),
    ('TKAMY', 'ThyssenKrupp AG', 'Germany', 'Aerospace & Defense', 'Industrial conglomerate, naval shipbuilding via Marine Systems'),
    ('FINMY', 'Finmeccanica / Leonardo SpA', 'Italy', 'Aerospace & Defense', 'Helicopters, electronics, defense systems'),
    ('NSKFF', 'Nippon Seiko (NSK Ltd.)', 'Japan', 'Industrials', 'Bearings manufacturer; defense relevance via precision components'),
    ('HAGHY', 'Hensoldt AG', 'Germany', 'Aerospace & Defense', 'Sensors, radar, electronic warfare systems'),
    ('INDRY', 'Indra Sistemas SA', 'Spain', 'Aerospace & Defense', 'Defense electronics, radar, C4ISR systems');

INSERT INTO symbols (ticker, company_name, exchange, asset_type, is_etf, is_active, date_loaded)
VALUES 
       ('BAESY', 'BAE Systems plc', 'OTC', 'stock', 0, 1, CURRENT_DATE),
       ('TKAMY', 'ThyssenKrupp AG', 'OTC', 'stock', 0, 1, CURRENT_DATE),
       ('FINMY', 'Leonardo SpA', 'OTC', 'stock', 0, 1, CURRENT_DATE),
       ('NSKFF', 'NSK Ltd.', 'OTC', 'stock', 0, 1, CURRENT_DATE),
       ('HAGHY', 'Hensoldt AG', 'OTC', 'stock', 0, 1, CURRENT_DATE),
       ('INDRY', 'Indra Sistemas SA', 'OTC', 'stock', 0, 1, CURRENT_DATE)
ON CONFLICT (ticker) DO NOTHING;

INSERT INTO stocks (ticker, company_name, exchange, industry, sector)
VALUES 
       ('BAESY', 'BAE Systems plc', 'OTC', 'Aerospace & Defense', 'Aerospace & Defense'),
       ('TKAMY', 'ThyssenKrupp AG', 'OTC', 'Aerospace & Defense', 'Aerospace & Defense'),
       ('FINMY', 'Leonardo SpA', 'OTC', 'Aerospace & Defense', 'Aerospace & Defense'),
       ('NSKFF', 'NSK Ltd.', 'OTC', 'Industrials', 'Industrials'),
       ('HAGHY', 'Hensoldt AG', 'OTC', 'Aerospace & Defense', 'Aerospace & Defense'),
       ('INDRY', 'Indra Sistemas SA', 'OTC', 'Aerospace & Defense', 'Aerospace & Defense')
ON CONFLICT (ticker) DO NOTHING;

-- Correct wrong ticker
update adr_whitelist set ticker = 'ISMAF' where ticker = 'INDRY';
update symbols set ticker = 'ISMAF' where ticker = 'INDRY';
update stocks set ticker = 'ISMAF' where ticker = 'INDRY';
