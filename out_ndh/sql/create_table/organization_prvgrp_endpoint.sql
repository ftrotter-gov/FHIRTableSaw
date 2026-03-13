CREATE TABLE IF NOT EXISTS "public"."organization_prvgrp_endpoint" (
  "id" bigserial NOT NULL,
  "organization_prvgrp_id" bigint NOT NULL,
  "endpoint_id" bigint NOT NULL,
  CONSTRAINT "organization_prvgrp_endpoint_pk" PRIMARY KEY ("id"),
  CONSTRAINT "organization_prvgrp_endpoint_uniq" UNIQUE NULLS NOT DISTINCT ("organization_prvgrp_id", "endpoint_id")
);
CREATE INDEX IF NOT EXISTS "organization_prvgrp_endpoint_organization_prvgrp_idx" ON "public"."organization_prvgrp_endpoint" ("organization_prvgrp_id");
CREATE INDEX IF NOT EXISTS "organization_prvgrp_endpoint_endpoint_idx" ON "public"."organization_prvgrp_endpoint" ("endpoint_id");
