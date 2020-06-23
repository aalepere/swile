SELECT
	S.localization,
	S.firstName || " " || S.lastName as salesperson,
	S.attributionDate,
	S.num_signed,
	L.num_lost,
	round(1.0 * L.num_lost / S.num_signed,
	2) as ratio,
	G.gmv,
	G.num_order
FROM
	(
	select
		e.localization,
		e.firstName,
		e.lastName,
		o.attributionDate,
		count(distinct accountId) as num_signed
	from
		Employees e
	inner join Opportunities o on
		o.employeeId = e.id
	where
		o.status = "signed"
	group by
		e.localization,
		e.firstName,
		e.lastName,
		o.attributionDate) S
inner join (
	select
		e.localization,
		e.firstName,
		e.lastName,
		o.attributionDate,
		count(distinct accountId) as num_lost
	from
		Employees e
	inner join Opportunities o on
		o.employeeId = e.id
	where
		o.status = "lost"
	group by
		e.localization,
		e.firstName,
		e.lastName,
		o.attributionDate) L on
	S.localization = L.localization
	and S.firstName = L.firstName
	and S.lastName = L.lastName
	and S.attributionDate = L.attributionDate
inner join(
	select
		localization,
		firstName,
		lastName,
		month,
		count(a.accountId) as num_order,
		sum(grossBookings) as gmv
	from
		Employees e
	inner join Opportunities o on
		o.employeeId = e.id
	inner join AccountsActvity a on
		a.accountId = o.accountId
	group by
		localization,
		firstName,
		lastName,
		month) G on
	G.localization = S.localization
	and G.firstName = S.firstName
	and G.lastName = S.lastName
	and G.month = S.attributionDate
	order by gmv desc
