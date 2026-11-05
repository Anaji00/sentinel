'use client';

/**
 * The one text input. A label is required, and tied to the field: 7 of the app's 41
 * inputs had none, which leaves a screen reader announcing "edit text". A hint or an
 * error sits under the field and is linked to it, and an error is announced.
 *
 * `hideLabel` keeps the label for assistive tech when the layout already makes the
 * purpose obvious (a search box beside a button). `containerClassName` is for layout
 * (a width, flex sizing, a margin): it lands on the wrapper, because that is the element
 * that sits in the parent's row or column. `className` is for the field itself.
 */

import React from 'react';
import { inputClasses, type InputSize } from '../../lib/ui';

export interface InputProps extends Omit<React.ComponentPropsWithRef<'input'>, 'size'> {
  label: string;
  hideLabel?: boolean;
  hint?: string;
  error?: string | null;
  size?: InputSize;
  containerClassName?: string;
}

export function Input({
  label,
  hideLabel = false,
  hint,
  error,
  size = 'md',
  className,
  containerClassName,
  id,
  ...rest
}: InputProps) {
  const auto = React.useId();
  const fieldId = id ?? auto;
  const noteId = `${fieldId}-note`;
  const note = error || hint;
  return (
    <div className={['flex flex-col gap-1', containerClassName].filter(Boolean).join(' ')}>
      <label htmlFor={fieldId} className={hideLabel ? 'sr-only' : 'text-xs font-medium text-ink-dim'}>
        {label}
      </label>
      <input
        id={fieldId}
        aria-invalid={error ? true : undefined}
        aria-describedby={note ? noteId : undefined}
        className={inputClasses(size, Boolean(error), className)}
        {...rest}
      />
      {note && (
        <p id={noteId} role={error ? 'alert' : undefined} className={`text-micro ${error ? 'text-caution' : 'text-ink-mute'}`}>
          {note}
        </p>
      )}
    </div>
  );
}

export default Input;
