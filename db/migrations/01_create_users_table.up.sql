CREATE UNLOGGED TABLE IF NOT EXISTS  public.users (
		"id" uuid NOT NULL,
		"name" varchar(255) NOT NULL
	) WITH (
		OIDS=FALSE
	);