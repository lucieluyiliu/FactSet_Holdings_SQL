CREATE OR REPLACE FUNCTION quarter_add(q integer, x integer)
RETURNS integer AS $$
DECLARE
    myear integer;
    mquarter integer;
    years integer;
    quarters integer;
    result integer;
BEGIN
    myear := q / 100; -- Calculate the year
    mquarter := q % 10; -- Calculate the quarter
    years := x / 4; -- Calculate the full years to add
    quarters := x % 4; -- Calculate the remaining quarters to add

    -- Calculate the new quarter and year
    IF mquarter + quarters > 4 THEN
        result := (myear + years + 1) * 100 + (mquarter + quarters - 4);
    ELSE
        result := (myear + years) * 100 + (mquarter + quarters);
    END IF;

    RETURN result;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION semiyear_add(yyyyhh integer, x integer)
RETURNS integer AS $$
DECLARE
    myear integer;
    mhalf integer;
    years integer;
    halves integer;
    result integer;
BEGIN
    myear := yyyyhh / 100; -- Calculate the year
    mhalf := yyyyhh % 10; -- Calculate the half-year
    years := x / 2; -- Calculate the full years to add
    halves := x % 2; -- Calculate the remaining halves to add

    -- Calculate the new half-year and year
    IF mhalf + halves > 2 THEN
        result := (myear + years + 1) * 100 + (mhalf + halves - 2);
    ELSE
        result := (myear + years) * 100 + (mhalf + halves);
    END IF;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION month_add(yyyymm integer, x integer)
RETURNS integer AS $$
DECLARE
    myear integer;
    mmonth integer;
    years integer;
    months integer;
    result integer;
BEGIN
    myear := yyyymm / 100; -- Calculate the year
    mmonth := yyyymm % 100; -- Calculate the month
    years := x / 12; -- Calculate the full years to add
    months := x % 12; -- Calculate the remaining months to add

    -- Calculate the new month and year, adjusting for overflow
    IF mmonth + months > 12 THEN
        result := (myear + years + 1) * 100 + (mmonth + months - 12);
    ELSE
        result := (myear + years) * 100 + (mmonth + months);
    END IF;

    RETURN result;
END;
$$ LANGUAGE plpgsql;