CREATE TABLE partnership.dbo.clinic_map
(
  id         INTEGER IDENTITY (0,1) PRIMARY KEY,
  phc_id     INTEGER,
  epic_id    INTEGER,
  clinic_key VARCHAR(20) NOT NULL,
  clinic     VARCHAR(50) NOT NULL,
  county     VARCHAR(50) NOT NULL,
  is_active  BIT NOT NULL
);
