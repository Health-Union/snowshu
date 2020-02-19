/* supports only 1st syntax ie RLIKE(subject,pattern,args). 
Only supports the 'i' (case insensitive) flag at this time.*/

CREATE OR REPLACE FUNCTION RLIKE(subject text, pattern text, args text DEFAULT '')
RETURNS BOOLEAN AS 
$$
SELECT
	CASE 
		WHEN $3 = 'i' THEN $1 ~* $2
		ELSE $1 ~ $2
	end;
$$
LANGUAGE SQL;
