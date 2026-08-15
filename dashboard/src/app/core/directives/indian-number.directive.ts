import { Directive, ElementRef, HostListener, forwardRef } from '@angular/core';
import { ControlValueAccessor, NG_VALUE_ACCESSOR } from '@angular/forms';

/**
 * Displays a numeric text input using the Indian digit-grouping system (e.g. 12,34,567),
 * while keeping the bound ngModel a plain `number`.
 *
 * - When not focused: shows the value grouped (₹-style lakh/crore commas).
 * - When focused: shows plain digits so editing/selection is natural.
 * - Emits a `number | null` back to the model as the user types.
 *
 * Usage: `<input type="text" inputmode="numeric" appIndianNumber [(ngModel)]="amountInr" />`
 */
@Directive({
  selector: 'input[appIndianNumber]',
  standalone: true,
  providers: [
    { provide: NG_VALUE_ACCESSOR, useExisting: forwardRef(() => IndianNumberDirective), multi: true },
  ],
})
export class IndianNumberDirective implements ControlValueAccessor {
  private value: number | null = null;
  private focused = false;
  private onChange: (v: number | null) => void = () => {};
  private onTouched: () => void = () => {};

  private readonly formatter = new Intl.NumberFormat('en-IN', { maximumFractionDigits: 2 });

  constructor(private el: ElementRef<HTMLInputElement>) {}

  writeValue(v: number | null): void {
    this.value = v == null || isNaN(Number(v)) ? null : Number(v);
    this.render();
  }

  registerOnChange(fn: (v: number | null) => void): void {
    this.onChange = fn;
  }

  registerOnTouched(fn: () => void): void {
    this.onTouched = fn;
  }

  setDisabledState(isDisabled: boolean): void {
    this.el.nativeElement.disabled = isDisabled;
  }

  @HostListener('focus')
  onFocus(): void {
    this.focused = true;
    this.el.nativeElement.value = this.value == null ? '' : String(this.value);
  }

  @HostListener('blur')
  onBlur(): void {
    this.focused = false;
    this.onTouched();
    this.render();
  }

  @HostListener('input', ['$event'])
  onInput(event: Event): void {
    const parsed = this.parse((event.target as HTMLInputElement).value);
    this.value = parsed;
    this.onChange(parsed);
  }

  private parse(raw: string): number | null {
    const cleaned = String(raw ?? '').replace(/[^0-9.\-]/g, '');
    if (cleaned === '' || cleaned === '-' || cleaned === '.') return null;
    const n = Number(cleaned);
    return isNaN(n) ? null : n;
  }

  private render(): void {
    const el = this.el.nativeElement;
    if (this.focused) {
      el.value = this.value == null ? '' : String(this.value);
    } else {
      el.value = this.value == null ? '' : this.formatter.format(this.value);
    }
  }
}
