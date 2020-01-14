CREATE UNLOGGED TABLE IF NOT EXISTS public.messages (
		"id" uuid NOT NULL,
		"text" TEXT NOT NULL,
		"category_id" uuid NOT NULL,
		"posted_at" TIME NOT NULL,
		"author_id" uuid NOT NULL
	) WITH (
		OIDS=FALSE
	);