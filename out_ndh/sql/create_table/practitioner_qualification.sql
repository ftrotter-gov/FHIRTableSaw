CREATE TABLE IF NOT EXISTS "public"."practitioner_qualification" (
  "id" bigserial NOT NULL,
  "practitioner_parent_id" bigint NOT NULL,
  "idx" integer,
  "code_text" text,
  "issuer_display" text,
  CONSTRAINT "practitioner_qualification_pk" PRIMARY KEY ("id"),
  CONSTRAINT "practitioner_qualification_uniq" UNIQUE NULLS NOT DISTINCT ("practitioner_parent_id", "code_text", "issuer_display")
);
CREATE INDEX IF NOT EXISTS "practitioner_qualification_practitioner_parent_id_idx" ON "public"."practitioner_qualification" ("practitioner_parent_id");
