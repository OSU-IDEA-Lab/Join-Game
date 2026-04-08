-- Cars Dataset
CREATE TABLE Car_brands (
    Year INT,
    Make TEXT,
    Model TEXT,
    Category TEXT
);

CREATE TABLE parking_tickets (
    summons_number BIGINT PRIMARY KEY,
    plate_id TEXT,
    registration_state TEXT,
    plate_type TEXT,
    issue_date DATE,
    violation_code INT,
    vehicle_body_type TEXT,
    vehicle_make TEXT,
    violation_location TEXT,
    violation_precinct INT,
    vehicle_color TEXT
);

CREATE TABLE car_accidents (
    Report_Number TEXT,
    Local_Case_Number TEXT,
    Collision_Type TEXT,
    Weather TEXT,
    Person_ID TEXT,
    Vehicle_ID TEXT,
    Vehicle_Make TEXT,
    Vehicle_Model TEXT
);

-- WDC Dataset
CREATE TABLE wdc1Brands (
    id TEXT,
    title TEXT,
    description TEXT,
    brand TEXT,
    price TEXT,
    category TEXT,
    cluster_id TEXT
);

CREATE TABLE wdc2Brands (
    id TEXT,
    title TEXT,
    description TEXT,
    brand TEXT,
    price TEXT,
    category TEXT,
    cluster_id TEXT
);

CREATE TABLE wdc3Brands (
    id TEXT,
    title TEXT,
    description TEXT,
    brand TEXT,
    price TEXT,
    category TEXT,
    cluster_id TEXT
);

-- Amazon Book Review Dataset
CREATE TABLE Books_data (
    id INT,
    title TEXT,
    description TEXT,
    authors TEXT,
    image_url TEXT,
    preview_link TEXT,
    publisher TEXT,
    published_date DATE,
    info_link TEXT,
    categories TEXT
);

CREATE TABLE Reviews (
    id VARCHAR(13),
    title TEXT,
    price DECIMAL(15, 2),
    user_id VARCHAR(255),
    profile_name TEXT,
    review_helpfulness TEXT,
    review_score DECIMAL(2, 1),
    review_time DECIMAL(15, 2),
    review_summary TEXT,
    review_text TEXT
);