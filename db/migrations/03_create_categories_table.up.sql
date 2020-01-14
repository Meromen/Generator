CREATE UNLOGGED TABLE IF NOT EXISTS  public.categories (
		"id" uuid NOT NULL,
		"name" varchar(255) NOT NULL,
		"parent_id" uuid NOT NULL
	) WITH (
		OIDS=FALSE
	);