/** One row in the year-by-year projection table */
export interface FinancialSimRow {
  age: number;
  startingInr: number;
  plannedExpensesInr: number;
  additionalExpensesInr: number;
  /** All rental streams combined (nominal INR) for this year */
  rentalIncomeAnnualInr: number;
  additionalSavingsInr: number;
  endingInr: number;
  status: 'Earning' | 'Retired';
  warning: string;
}

/** User-defined goal (marriage, education, etc.) */
export interface FinancialRequirement {
  id: string;
  name: string;
  /** Simulation year index when this spend starts (0 = first year / current age) */
  yearsFromNow: number;
  /** How many consecutive years `yearlyAmountTodaysInr` applies */
  durationYears: number;
  /** Annual spend in “today’s” INR; inflated each year by the global inflation rate */
  yearlyAmountTodaysInr: number;
}

/** One bucket in the investment mix */
export interface PortfolioBucket {
  label: string;
  returnPct: number;
  taxPct: number;
  sharePct: number;
}

/** Rental (or similar) income from a given plan year through end of life */
export interface RentalIncomeStream {
  id: string;
  label: string;
  /** Monthly amount in today’s INR when the stream starts */
  monthlyTodaysInr: number;
  /** First plan year index when rent is received (0 = year 1 of table) */
  startsInYears: number;
  /** Total nominal annual growth rate % (0 = grows with inflation only; 10 = 10% per year total) */
  incrementPctPerYear: number;
}

/** One monthly expense line; add/remove like rental streams */
export interface MonthlyExpenseStream {
  id: string;
  label: string;
  /** Monthly amount in today’s INR */
  monthlyTodaysInr: number;
  category: 'need' | 'want';
  /** 0 = entire projection; N &gt; 0 = only plan year indices 0 … N−1 */
  endsAfterYears: number;
}

/**
 * Salary / bonus-monthly style income while earning. Not paid after retirement age.
 * `earningYearsCap`: 0 = every earning year until retirement; N &gt; 0 = only plan years 0 … N−1 (while earning).
 */
export interface MonthlyIncomeStream {
  id: string;
  label: string;
  monthlyTodaysInr: number;
  earningYearsCap: number;
}

/** One-off yearly income (bonuses etc.), today’s INR; inflated each year in surplus math */
export interface YearlyIncomeStream {
  id: string;
  label: string;
  yearlyTodaysInr: number;
}

/** Yearly lump expense (travel etc.), today’s INR; inflated each year */
export interface YearlyExpenseStream {
  id: string;
  label: string;
  yearlyTodaysInr: number;
}
