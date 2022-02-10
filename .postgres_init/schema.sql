CREATE TABLE "public"."wallets" (
    "name" text NOT NULL,
    "amount" int8 NOT NULL DEFAULT 0 CHECK (amount >= (0)::bigint),
    "create_time" timestamp NOT NULL DEFAULT now(),
    PRIMARY KEY ("name")
);

CREATE TABLE "public"."on_call" (
    "name" text NOT NULL,
    "on_call" boolean NOT NULL DEFAULT false,
    "shift" text NOT NULL,
    PRIMARY KEY ("name")
);