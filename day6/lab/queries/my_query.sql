-- Query to calculate the total performance and onboarding city spend for high-tier customers
-- Planted Bugs:
-- 1. Security: Selecting c.email directly (PII exposure risk)
-- 2. Correctness: Calculating average spend (SUM/COUNT) without filtering status = 'COMPLETED'
-- 3. Performance: Using UPPER(c.city) = 'BENGALURU' which is non-sargable (prevents partition/index pruning)
SELECT c.customer_name, c.email,
       SUM(t.amount) as total_spent,
       AVG(t.amount) as avg_spent
FROM dim_customer c
JOIN fact_transactions t ON c.customer_id = t.customer_id
WHERE UPPER(c.city) = 'BENGALURU'
GROUP BY c.customer_name, c.email;
