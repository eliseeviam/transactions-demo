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

CREATE TABLE "public"."coupons" (
    "name" text NOT NULL,
    "applied" boolean NOT NULL DEFAULT false,
    PRIMARY KEY ("name")
);

CREATE TABLE "public"."jobs" (
    "name" text NOT NULL,
    "description" text NOT NULL,
    "done" boolean NOT NULL DEFAULT false,
    PRIMARY KEY ("name")
);

CREATE TABLE "public"."accounts" (
    "owner" int8 NOT NULL,
    "name" text NOT NULL,
    "inited" boolean NOT NULL DEFAULT false
);