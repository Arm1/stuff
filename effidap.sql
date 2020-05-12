insert into esc_effidap_owed(supplier_id, entry_date, production_line_code, conception_code, section_id, conception_name, opening_time, total_labour, department_desc, section_desc, typo_proc_desc, tlt_line, tst_line_sam, tst_line_sot, tst_sam, tst_sot, effi_line_sam, effi_line_sot, version_code, tlt_sot, tlt_sam, goods_outputs, supplier_name, dpp_label, univers_label, sam, sot, direct_labour, indirect_labour, planned_stop, unplanned_stop, reworks, rejects, problems, target_owe)
select supplier_id, entry_date, production_line_code, conception_code, section_id, conception_name, opening_time, total_labour, department_desc, section_desc, typo_proc_desc, tlt_line, tst_line_sam, tst_line_sot, tst_sam, tst_sot, effi_line_sam, effi_line_sot, version_code, tlt_sot, tlt_sam, goods_outputs, supplier_name, dpp_label, univers_label, sam, sot, direct_labour, indirect_labour, planned_stop, unplanned_stop, reworks, rejects, problems, target_owe
from (
select *, (tst_line_sot / tlt_line) as effi_line_sot, (tst_line_sam / tlt_line) as
effi_line_sam,  (tst_sot * tlt_line / tst_line_sot) as tlt_sot, (tst_sam * tlt_line / tst_line_sam) as tlt_sam
from  (  select  supplier_id, cla.cla_technical_label supplier_name,
( select ed.dpp_label  from esc_effidap_dpp ed  where  ed.dpp_id = cla.cla_id_dpp) dpp_label,  
( select eu.univers_desc  from  esc_effidap_univers eu  where eu.univers_code = cla.cla_code_univers) univers_label,  
entry_date,  production_line_code, 
case when sum(opening_time) is null then ( select  max(opt.opening_time)  from esc_effidap_data_entry opt where opt.supplier_id = supplier_id and 
	opt.entry_date = entry_date  and opt.production_line_code = production_line_code and opt.opening_time is not null )
else 
	sum(opening_time) 
end as opening_time_sum,
case when sum(total_labour) is null then ( select  max(opt.total_labour)  from esc_effidap_data_entry opt where opt.supplier_id = supplier_id and
	opt.entry_date = entry_date  and opt.production_line_code = production_line_code and opt.total_labour is not null )  
else sum(total_labour)
end as total_labour_sum,
( select  department_desc  from  esc_effidap_department  where department_id = cfg.department_id),  
cfgline.section_id,  (  select  section_desc from  esc_effidap_section  where  section_id = cfgline.section_id),  
(  select typo_proc_desc  from  esc_effidap_typo_process  where typo_proc_id = samsotLine.typo_proc_id),  
conception_name as conception_code ,  
( select distinct first_value(ei.it_cc_name) over(partition by it_code_cc)  
from  esr_item ei  where ei.it_code_cc = conception_name  and it_cc_name 
is not null ) as conception_name, sum(goods_outputs) goods_outputs,  
(goods_outputs * sum(sot)) as tst_sot, 
case (sum(goods_outputs* sot) over (partition by supplier_id,  entry_date,  production_line_code, conception_name, version_code))
when 0 then 1  else (sum(goods_outputs* sot) over (partition by supplier_id,  entry_date, production_line_code, conception_name, version_code))
end as tst_line_sot,
(goods_outputs * sum(sam)) as tst_sam, 
case (sum(goods_outputs* sam) over (partition by supplier_id,  entry_date, production_line_code, conception_name, version_code)) 
when 0 then 1  else (sum(goods_outputs* sam) over (partition by supplier_id,  entry_date, production_line_code, conception_name, version_code))  
end as tst_line_sam,
case ((case when sum(opening_time) is null then (  select  max(opt.opening_time)  from esc_effidap_data_entry opt  where  
	opt.supplier_id = supplier_id and opt.entry_date = entry_date  and opt.production_line_code = production_line_code and 
	opt.opening_time is not null )
	else 
	sum(opening_time) end) *  (case when sum(total_labour) is null 
	then (  select  max(opt.total_labour)  from esc_effidap_data_entry opt  where  opt.supplier_id = supplier_id and opt.entry_date = entry_date  and opt.production_line_code = production_line_code and opt.total_labour is not null )  
	else sum(total_labour)  end))  
when 0 then 1  
else 
	((case  when sum(opening_time) is null then (  select  max(opt.opening_time)  from esc_effidap_data_entry opt  
	where  opt.supplier_id = supplier_id and 
	opt.entry_date = entry_date  and opt.production_line_code = production_line_code and opt.opening_time is not null )
	else sum(opening_time)  end) *  (case when sum(total_labour) is null then (select  max(opt.total_labour)  
	from esc_effidap_data_entry opt  where  opt.supplier_id = supplier_id and opt.entry_date = entry_date  and 
	opt.production_line_code = production_line_code and opt.total_labour is not null )  else sum(total_labour)  end)) 
end as tlt_line ,
 version_code,  sum(sam) as sam,  sum(sot) as sot,  dataEntry.direct_labour, dataEntry.indirect_labour, 
dataEntry.total_labour,  dataEntry.opening_time, dataEntry.planned_stop,  dataEntry.unplanned_stop,  dataEntry.reworks,  
dataEntry.rejects,  samsotLine.target_owe, dataEntry.problems from  esc_effidap_data_entry as dataEntry 
inner join esc_effidap_supplier_config cfg on cfg.line_code = dataEntry.production_line_code  and 
cfg.location_id = dataEntry.supplier_id 
inner join esc_effidap_suppplier_config_line cfgLine on cfgLine.supp_config_id = cfg.supp_config_id  
inner join esc_effidap_sam_sot samsot on regexp_replace(code_cc, '^0+', '') = regexp_replace(conception_name, '^0+', '') 
inner join esc_effidap_sam_sot_line samsotLine on  samsot.sam_sot_id = samsotLine.sam_sot_id and 
samsotLine.section_id = cfgLine.section_id  
inner join esc_effidap_sam_sot_version vers on (samsot.sam_sot_version_id = vers.sam_sot_version_id 
and vers.location_id = dataEntry.supplier_id 
and vers.version_status = 3 -- and vers.version_code in('V_1','V_4')
) 
inner join esr_customer_location_area cla on  cla.cla_id = supplier_id 
where 
dataEntry.supplier_id = 53 and 
dataEntry.entry_date >= to_date('2020-03-30', 'YYYY-MM-dd') 
group by  supplier_id,  entry_date,  
production_line_code,  conception_name, version_code,  goods_outputs,  sot,  cfg.department_id,  
cfgline.section_id, samsotLine.typo_proc_id,  sam,  cla.cla_technical_label,  cla.cla_id_dpp, cla.cla_code_univers,
dataEntry.direct_labour,  dataEntry.indirect_labour, dataEntry.total_labour,  dataEntry.opening_time,  dataEntry.planned_stop,
dataEntry.unplanned_stop,  dataEntry.reworks, dataEntry.rejects, samsotLine.target_owe,  dataEntry.problems) 
as data_line
) as test