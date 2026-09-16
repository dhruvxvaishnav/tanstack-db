import { createClient, type SupabaseClient } from "@supabase/supabase-js"
import { eq, IR, type LoadSubsetOptions } from "@tanstack/db"
import { QueryClient } from "@tanstack/query-core"
import { describe, expect, test, vi } from "vitest"
import { supabaseQueryFn } from "../src/functions"
import { supabaseCollectionOptions } from "../src/index"
import {
  normalizeFetchUrl,
  SUPABASE_KEY,
  SUPABASE_URL,
  usersSchema,
} from "./test.utils"

interface TestRow {
  active: boolean
  email: string
  id: number
  name: string
  [key: string]: unknown
}

type Row = Record<string, unknown>

const makeRows = (count: number): TestRow[] =>
  Array.from({ length: count }, (_, id) => ({
    active: true,
    email: `user-${id}@test.com`,
    id,
    name: `User ${id}`,
  }))

// ── A small PostgREST-like mock ──────────────────────────────────────
// Parses `order` and simple top-level `col=eq.value` filters with the same
// typing PostgREST would use, then slices by offset/limit (capped at the
// simulated server row cap) and reports `Content-Range` for the filtered
// total, matching what a real `count=exact` request returns.

const RESERVED_PARAMS = new Set(["select", "order", "limit", "offset"])

const compareValues = (a: unknown, b: unknown): number => {
  if (typeof a === "number" && typeof b === "number") return a - b
  if (typeof a === "boolean" && typeof b === "boolean") {
    if (a === b) return 0
    return a ? 1 : -1
  }
  const left = String(a)
  const right = String(b)
  if (left < right) return -1
  if (left > right) return 1
  return 0
}

const unquote = (raw: string): string => {
  if (raw.startsWith('"') && raw.endsWith('"')) {
    return raw.slice(1, -1).replace(/\\(.)/g, "$1")
  }
  return raw
}

const coerce = (raw: string, sample: unknown): unknown => {
  if (typeof sample === "number") return Number(raw)
  if (typeof sample === "boolean") return raw === "true"
  return unquote(raw)
}

type Predicate = (row: Row) => boolean

const parseLeaf = (token: string): Predicate => {
  const firstDot = token.indexOf(".")
  const column = token.slice(0, firstDot)
  const rest = token.slice(firstDot + 1)
  if (rest === "is.null") {
    return (row) => row[column] === null
  }
  if (rest === "not.is.null") {
    return (row) => row[column] !== null
  }
  const opDot = rest.indexOf(".")
  const op = rest.slice(0, opDot)
  const rawValue = rest.slice(opDot + 1)
  return (row) => {
    const sample = row[column]
    const value = coerce(rawValue, sample)
    if (op === "eq") return sample === value
    if (op === "gt") return (sample as never) > (value as never)
    if (op === "lt") return (sample as never) < (value as never)
    throw new Error(`unsupported operator in test mock: ${op}`)
  }
}

const parseOrder = (
  orderParam: string | null
): Array<{ ascending: boolean; column: string }> => {
  if (!orderParam) return []
  return orderParam.split(",").map((part) => {
    const [column, direction] = part.split(".")
    return { ascending: direction !== "desc", column }
  })
}

const sortRows = <T extends Row>(
  rows: T[],
  order: ReturnType<typeof parseOrder>
): T[] =>
  [...rows].sort((a, b) => {
    for (const { ascending, column } of order) {
      const cmp = compareValues(a[column], b[column])
      if (cmp !== 0) return ascending ? cmp : -cmp
    }
    return 0
  })

const createPagedFetch = <T extends Row>(
  rows: T[],
  {
    errorOnRequest,
    maxRows = 1000,
    omitCount = false,
    onRequest,
  }: {
    errorOnRequest?: number
    maxRows?: number
    omitCount?: boolean
    onRequest?: (index: number) => void
  } = {}
) => {
  let requestCount = -1
  return vi.fn<typeof fetch>().mockImplementation((input) => {
    requestCount += 1
    const index = requestCount
    onRequest?.(index)

    const url = new URL(typeof input === "string" ? input : input.toString())
    const params = url.searchParams

    if (index === errorOnRequest) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            code: "PGRST000",
            details: null,
            hint: null,
            message: "page failed",
          }),
          { status: 500, headers: { "content-type": "application/json" } }
        )
      )
    }

    let filtered = rows.filter((row) => {
      for (const [key, value] of params.entries()) {
        if (RESERVED_PARAMS.has(key)) continue
        if (!parseLeaf(`${key}.${value}`)(row)) return false
      }
      return true
    })

    filtered = sortRows(filtered, parseOrder(params.get("order")))

    const total = filtered.length
    const offset = Number(params.get("offset") ?? 0)
    const requestedLimit = Number(params.get("limit") ?? maxRows)
    const limit = Math.min(requestedLimit, maxRows)
    const page = filtered.slice(offset, offset + limit)

    const headers: Record<string, string> = {
      "content-type": "application/json",
    }
    if (!omitCount) {
      headers["content-range"] =
        page.length === 0
          ? `*/${total}`
          : `${offset}-${offset + page.length - 1}/${total}`
    }

    return Promise.resolve(
      new Response(JSON.stringify(page), { status: 200, headers })
    )
  })
}

const runQuery = (
  supabase: SupabaseClient,
  loadSubsetOptions: LoadSubsetOptions = {},
  pageSize?: number,
  keys = ["id"],
  signal: AbortSignal = new AbortController().signal
) =>
  supabaseQueryFn(
    supabase,
    "users",
    keys,
    {
      client: {} as never,
      queryKey: ["users"],
      signal,
      meta: { loadSubsetOptions },
    },
    pageSize
  )

const sort = (column: string, direction: "asc" | "desc" = "asc") => ({
  expression: new IR.PropRef([column]),
  compareOptions: { direction, nulls: "last" as const },
})

const queryOptions = (): LoadSubsetOptions => ({
  orderBy: [sort("id")],
  where: eq(new IR.PropRef<boolean>(["active"]), true),
})

const supabaseWithFetch = (mockFetch: typeof fetch) =>
  createClient(SUPABASE_URL, SUPABASE_KEY, { global: { fetch: mockFetch } })

describe("collection query pagination", () => {
  test("continues past a server cap lower than pageSize", async () => {
    const expected = makeRows(3000)
    const mockFetch = createPagedFetch(expected, { maxRows: 500 })
    const supabase = supabaseWithFetch(mockFetch)

    const rows = await runQuery(supabase)

    expect(rows).toEqual(expected)
    expect(mockFetch).toHaveBeenCalledTimes(6)
    for (const [url] of mockFetch.mock.calls) {
      expect(new URL(String(url)).searchParams.get("limit")).toBe("1000")
    }
    // Regression test: the server cap (500) is below pageSize (1000), so each
    // page only returns 500 rows. The offset must advance by rows actually
    // received, not by the requested limit, or the next page would skip 500
    // rows every time.
    expect(
      mockFetch.mock.calls.map(([url]) =>
        new URL(String(url)).searchParams.get("offset")
      )
    ).toEqual([null, "500", "1000", "1500", "2000", "2500"])
  })

  test("does not change behavior when the server cap exceeds pageSize (no-op)", async () => {
    const expected = makeRows(3501)
    const mockFetch = createPagedFetch(expected, { maxRows: 2000 })
    const supabase = supabaseWithFetch(mockFetch)

    const rows = await runQuery(supabase)

    expect(rows).toEqual(expected)
    expect(mockFetch).toHaveBeenCalledTimes(4)
    for (const [url] of mockFetch.mock.calls) {
      expect(new URL(String(url)).searchParams.get("limit")).toBe("1000")
    }
  })

  test("stops without an extra request when the row count is an exact multiple of pageSize", async () => {
    const expected = makeRows(2000)
    const mockFetch = createPagedFetch(expected)
    const supabase = supabaseWithFetch(mockFetch)

    const rows = await runQuery(supabase, {}, 1000)

    expect(rows).toEqual(expected)
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })

  describe("falls back to an under-full page when the server reports no count", () => {
    test("stops on the first under-full page", async () => {
      const expected = makeRows(3)
      const mockFetch = createPagedFetch(expected, { omitCount: true })
      const supabase = supabaseWithFetch(mockFetch)

      const rows = await runQuery(supabase, {}, 2)

      expect(rows).toEqual(expected)
      expect(mockFetch).toHaveBeenCalledTimes(2)
    })

    test("issues a trailing empty request for an exact multiple", async () => {
      const expected = makeRows(4)
      const mockFetch = createPagedFetch(expected, { omitCount: true })
      const supabase = supabaseWithFetch(mockFetch)

      const rows = await runQuery(supabase, {}, 2)

      expect(rows).toEqual(expected)
      expect(mockFetch).toHaveBeenCalledTimes(3)
    })
  })

  test.each([
    { keys: ["id"], orderBy: undefined, expected: "id.asc" },
    {
      keys: ["id"],
      orderBy: [sort("active", "desc")],
      expected: "active.desc,id.asc",
    },
    {
      keys: ["id"],
      orderBy: [sort("active"), sort("id", "desc")],
      expected: "active.asc,id.desc",
    },
    {
      keys: ["email", "id"],
      orderBy: undefined,
      expected: "email.asc,id.asc",
    },
    {
      keys: ["email", "id"],
      orderBy: [sort("email", "desc")],
      expected: "email.desc,id.asc",
    },
  ])("uses unique ordering $expected on every page", async ({
    keys,
    orderBy,
    expected,
  }) => {
    const mockFetch = createPagedFetch(makeRows(5))
    const supabase = supabaseWithFetch(mockFetch)

    await runQuery(supabase, { orderBy }, 2, keys)

    expect(mockFetch).toHaveBeenCalledTimes(3)
    for (const [url] of mockFetch.mock.calls) {
      expect(new URL(String(url)).searchParams.get("order")).toBe(expected)
    }
  })

  test("fetches all rows across multiple PostgREST pages", async () => {
    const mockFetch = createPagedFetch(makeRows(2501))
    const supabase = supabaseWithFetch(mockFetch)

    const rows = await runQuery(supabase)

    expect(rows).toHaveLength(2501)
    expect(mockFetch.mock.calls.map(([url]) => normalizeFetchUrl(url))).toEqual(
      [
        "/rest/v1/users?limit=1000&order=id.asc&select=*",
        "/rest/v1/users?limit=1000&offset=1000&order=id.asc&select=*",
        "/rest/v1/users?limit=1000&offset=2000&order=id.asc&select=*",
      ]
    )
  })

  test("preserves filters, ordering, limits, and offsets on every page", async () => {
    const mockFetch = createPagedFetch(makeRows(12))
    const supabase = supabaseWithFetch(mockFetch)

    const rows = await runQuery(
      supabase,
      { ...queryOptions(), limit: 5, offset: 3 },
      2
    )

    expect(rows.map(({ id }) => id)).toEqual([3, 4, 5, 6, 7])
    expect(mockFetch.mock.calls.map(([url]) => normalizeFetchUrl(url))).toEqual(
      [
        "/rest/v1/users?active=eq.true&limit=2&offset=3&order=id.asc&select=*",
        "/rest/v1/users?active=eq.true&limit=2&offset=5&order=id.asc&select=*",
        "/rest/v1/users?active=eq.true&limit=1&offset=7&order=id.asc&select=*",
      ]
    )
  })

  test("stops after a short final page", async () => {
    const mockFetch = createPagedFetch(makeRows(3), { maxRows: 2 })
    const supabase = supabaseWithFetch(mockFetch)

    const rows = await runQuery(supabase, {}, 2)

    expect(rows).toHaveLength(3)
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })

  test("does not request beyond an explicit limit", async () => {
    const mockFetch = createPagedFetch(makeRows(8), { maxRows: 2 })
    const supabase = supabaseWithFetch(mockFetch)

    const rows = await runQuery(supabase, { limit: 4 }, 2)

    expect(rows).toHaveLength(4)
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })

  test("returns no rows and makes no request for limit 0", async () => {
    const mockFetch = createPagedFetch(makeRows(8))
    const supabase = supabaseWithFetch(mockFetch)

    const rows = await runQuery(supabase, { limit: 0 }, 2)

    expect(rows).toEqual([])
    expect(mockFetch).not.toHaveBeenCalled()
  })

  test("sends Prefer: count=exact on every page", async () => {
    const mockFetch = createPagedFetch(makeRows(5))
    const supabase = supabaseWithFetch(mockFetch)

    await runQuery(supabase, {}, 2)

    expect(mockFetch.mock.calls.length).toBeGreaterThan(0)
    for (const [, init] of mockFetch.mock.calls) {
      const headers = new Headers(
        init?.headers as ConstructorParameters<typeof Headers>[0]
      )
      expect(headers.get("prefer")).toContain("count=exact")
    }
  })

  test("stops issuing requests once the signal is aborted", async () => {
    const controller = new AbortController()
    const mockFetch = createPagedFetch(makeRows(10), {
      onRequest: (index) => {
        if (index === 0) controller.abort()
      },
    })
    const supabase = supabaseWithFetch(mockFetch)

    await expect(
      runQuery(supabase, {}, 2, ["id"], controller.signal)
    ).rejects.toBeTruthy()
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })

  test("fails the complete load when a later page errors", async () => {
    const mockFetch = createPagedFetch(makeRows(5), { errorOnRequest: 1 })
    const supabase = supabaseWithFetch(mockFetch)

    await expect(runQuery(supabase, {}, 2)).rejects.toMatchObject({
      message: "page failed",
    })
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })
})

describe("pageSize validation", () => {
  test.each([
    0,
    -1,
    1.5,
    Number.NaN,
  ])("throws for an invalid pageSize (%s)", (pageSize) => {
    const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)
    expect(() =>
      supabaseCollectionOptions({
        tableName: "users",
        keys: ["id"],
        schema: usersSchema,
        supabase,
        queryClient: new QueryClient(),
        pageSize,
      })
    ).toThrow(/pageSize must be a positive integer/)
  })

  // `undefined` and `null` both fall back to the default page size.
  test.each([
    undefined,
    null,
  ])("uses the default when pageSize is %s", (pageSize) => {
    const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)
    expect(() =>
      supabaseCollectionOptions({
        tableName: "users",
        keys: ["id"],
        schema: usersSchema,
        supabase,
        queryClient: new QueryClient(),
        pageSize: pageSize as unknown as number,
      })
    ).not.toThrow()
  })
})
